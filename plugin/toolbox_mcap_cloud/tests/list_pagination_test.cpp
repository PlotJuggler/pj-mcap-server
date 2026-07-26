// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC tests for the ListFiles pagination sweep: the requested page size and
// the PROGRESSIVE per-page delivery callback.
//
// Why these exist. A real catalog browse is 25,550 files. Measured against a
// remote server (RTT ~37 ms) the sweep costs `pages * RTT + rows * 0.18 ms`, so:
//   * leaving ListFilesRequest.limit unset takes the server default of 200,
//     costing 128 round trips (~4.8 s of pure latency) where 52 would do;
//   * listSequences() returned only after the LAST page, so the dialog showed an
//     empty table for the whole sweep even though page one arrived in ~110 ms.
//
// The subtle part is the reset. A builder rebuild landing mid-pagination makes
// the server reject the next page with ERROR_STALE_CATALOG; the client then
// DISCARDS every page of that attempt and restarts from page one on the new
// generation. A progressive consumer therefore cannot be append-only — it must
// be told to drop what it has already drawn, or a rebuild during a browse leaves
// duplicated rows on screen.
//
// Same in-process ix::WebSocketServer shape as server_caps_test.cpp: no env
// gate, no external process, loopback ephemeral port.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include <ixwebsocket/IXWebSocketServer.h>

#include "backend_connection.hpp"
#include "pj_cloud.pb.h"

namespace {

int findFreePort() {
  const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ::bind(probe, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  ::getsockname(probe, reinterpret_cast<sockaddr*>(&addr), &len);
  const int port = ntohs(addr.sin_port);
  ::close(probe);
  return port;
}

// Serves `total_rows` files in pages of whatever limit the client asks for
// (clamped like the real server), records every requested limit, and can inject
// ONE ERROR_STALE_CATALOG at a chosen page index to force the restart path.
class FakePagingServer {
 public:
  FakePagingServer(int total_rows, int stale_at_page)
      : port_(findFreePort()), server_(port_, "127.0.0.1"), total_rows_(total_rows),
        stale_at_page_(stale_at_page) {
    server_.setOnClientMessageCallback([this](std::shared_ptr<ix::ConnectionState>, ix::WebSocket& ws,
                                              const ix::WebSocketMessagePtr& msg) {
      if (msg->type != ix::WebSocketMessageType::Message) {
        return;
      }
      pj_cloud::v1::ClientMessage request;
      if (!request.ParseFromString(msg->str)) {
        return;
      }
      pj_cloud::v1::ServerMessage response;
      response.set_request_id(request.request_id());

      if (request.has_hello()) {
        response.mutable_hello_response()->set_server_version("fake-paging-1.0");
      } else if (request.has_get_vocabulary()) {
        auto* vocab = response.mutable_get_vocabulary();
        auto* customer = vocab->add_customers();
        customer->set_id(11);
        customer->set_name("dexory");
        customer->set_file_count(static_cast<std::uint64_t>(total_rows_));
        auto* site_a = customer->add_sites();
        site_a->set_id(21);
        site_a->set_name("nashville");
        site_a->set_file_count(static_cast<std::uint64_t>(total_rows_ / 2));
        auto* site_b = customer->add_sites();
        site_b->set_id(22);
        site_b->set_name("wallingford");
        site_b->set_file_count(static_cast<std::uint64_t>(total_rows_ - total_rows_ / 2));
        vocab->set_catalog_generation("gen-1");
      } else if (request.has_list_files()) {
        const auto& req = request.list_files();
        const int page_index = req.page_token().empty() ? 0 : std::stoi(req.page_token());
        {
          std::lock_guard<std::mutex> lock(mu_);
          limits_seen_.push_back(req.limit());
        }
        // Inject the rebuild race exactly once, on the requested page.
        if (page_index == stale_at_page_ && !stale_fired_.exchange(true)) {
          auto* err = response.mutable_error();
          err->set_code(pj_cloud::v1::ERROR_STALE_CATALOG);
          err->set_message("catalog rebuilt mid-pagination");
          std::string payload;
          response.SerializeToString(&payload);
          ws.sendBinary(payload);
          return;
        }
        const int limit = req.limit() == 0 ? 200 : std::min<int>(req.limit(), 1000);
        auto* list = response.mutable_list_files();
        list->set_catalog_generation("gen-1");
        const int start = page_index * limit;
        const int end = std::min(start + limit, total_rows_);
        for (int i = start; i < end; ++i) {
          auto* file = list->add_files();
          file->set_id(static_cast<std::uint64_t>(i + 1));
          file->set_s3_key("file_" + std::to_string(i) + ".mcap");
        }
        if (end < total_rows_) {
          list->set_next_page_token(std::to_string(page_index + 1));
        }
      } else {
        return;
      }
      std::string payload;
      response.SerializeToString(&payload);
      ws.sendBinary(payload);
    });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~FakePagingServer() { server_.stop(); }

  [[nodiscard]] bool ok() const { return ok_; }
  [[nodiscard]] std::string uri() const { return "ws://127.0.0.1:" + std::to_string(port_); }

  [[nodiscard]] std::vector<std::uint32_t> limitsSeen() const {
    std::lock_guard<std::mutex> lock(mu_);
    return limits_seen_;
  }

 private:
  int port_;
  ix::WebSocketServer server_;
  int total_rows_;
  int stale_at_page_;
  std::atomic<bool> stale_fired_{false};
  bool ok_ = false;
  mutable std::mutex mu_;
  std::vector<std::uint32_t> limits_seen_;
};

// One recorded invocation of the progressive page callback.
struct PageEvent {
  std::size_t rows;
  bool reset;
};

}  // namespace

// --- page size -------------------------------------------------------------

TEST(ListPagination, RequestsAnExplicitPageLimit) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  bool complete = false;
  const auto sequences = conn.listSequences(&complete);
  ASSERT_TRUE(complete);
  EXPECT_EQ(sequences.size(), 1200u);

  // Every page must carry the explicit limit; an unset (0) limit silently takes
  // the server's 200 default and triples the round trips.
  const auto limits = server.limitsSeen();
  ASSERT_FALSE(limits.empty());
  for (const auto limit : limits) {
    EXPECT_EQ(limit, mcap_cloud::kListFilesPageLimit);
  }
  EXPECT_GT(mcap_cloud::kListFilesPageLimit, 200u) << "must beat the server default";
  EXPECT_LE(mcap_cloud::kListFilesPageLimit, 1000u) << "server clamps above 1000";
}

// --- progressive delivery --------------------------------------------------

TEST(ListPagination, DeliversEachPageAsItArrives) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  std::vector<PageEvent> events;
  bool complete = false;
  const auto sequences = conn.listSequences(
      &complete, [&](const std::vector<mcap_cloud::SequenceInfo>& page, bool reset) {
        events.push_back({page.size(), reset});
      });

  ASSERT_TRUE(complete);
  EXPECT_EQ(sequences.size(), 1200u);
  // 1200 rows at the client's page size -> more than one page, and the pages
  // must tile the full result exactly.
  ASSERT_GT(events.size(), 1u);
  std::size_t delivered = 0;
  for (const auto& e : events) {
    delivered += e.rows;
  }
  EXPECT_EQ(delivered, sequences.size());
}

TEST(ListPagination, OnlyTheFirstPageIsMarkedReset) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  std::vector<PageEvent> events;
  const auto sequences =
      conn.listSequences(nullptr, [&](const std::vector<mcap_cloud::SequenceInfo>& page, bool reset) {
        events.push_back({page.size(), reset});
      });
  ASSERT_EQ(sequences.size(), 1200u);

  ASSERT_GT(events.size(), 1u);
  EXPECT_TRUE(events.front().reset) << "consumer must clear before the first page";
  for (std::size_t i = 1; i < events.size(); ++i) {
    EXPECT_FALSE(events[i].reset) << "page " << i << " must append, not clear";
  }
}

TEST(ListPagination, StaleCatalogRestartSignalsASecondReset) {
  // Rebuild lands while page 2 is being fetched: the client discards the pages
  // it already delivered and restarts from page one, so the consumer MUST get a
  // second reset or it would render the first attempt's rows twice.
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  std::vector<PageEvent> events;
  bool complete = false;
  const auto sequences = conn.listSequences(
      &complete, [&](const std::vector<mcap_cloud::SequenceInfo>& page, bool reset) {
        events.push_back({page.size(), reset});
      });

  ASSERT_TRUE(complete) << "one stale retry is within the bounded budget";
  EXPECT_EQ(sequences.size(), 1200u);

  int resets = 0;
  for (const auto& e : events) {
    if (e.reset) {
      ++resets;
    }
  }
  EXPECT_EQ(resets, 2) << "one for the initial attempt, one for the restart";

  // Rows delivered AFTER the final reset must tile the returned vector exactly.
  std::size_t after_last_reset = 0;
  for (const auto& e : events) {
    if (e.reset) {
      after_last_reset = 0;
    }
    after_last_reset += e.rows;
  }
  EXPECT_EQ(after_last_reset, sequences.size());
}

TEST(ListPagination, WorksWithoutACallback) {
  // Backward compatibility: every existing caller passes no callback.
  FakePagingServer server(/*total_rows=*/700, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  bool complete = false;
  const auto sequences = conn.listSequences(&complete);
  EXPECT_TRUE(complete);
  EXPECT_EQ(sequences.size(), 700u);
}

// --- vocabulary --------------------------------------------------------

TEST(Vocabulary, MapsTheCustomerSiteTree) {
  FakePagingServer server(/*total_rows=*/1000, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  const auto vocab = conn.getVocabulary();
  ASSERT_TRUE(vocab.has_value());
  EXPECT_EQ(vocab->generation, "gen-1");
  ASSERT_EQ(vocab->customers.size(), 1u);
  EXPECT_EQ(vocab->customers[0].name, "dexory");
  EXPECT_EQ(vocab->customers[0].id, 11u);
  ASSERT_EQ(vocab->customers[0].sites.size(), 2u);
  EXPECT_EQ(vocab->customers[0].sites[0].name, "nashville");
  EXPECT_EQ(vocab->customers[0].sites[0].id, 21u);
  EXPECT_EQ(vocab->customers[0].sites[0].file_count, 500u);
  EXPECT_EQ(vocab->totalFiles(), 1000u);
  EXPECT_EQ(vocab->totalSites(), 2u);
}

TEST(Vocabulary, FailsCleanlyWhenNotConnected) {
  mcap_cloud::BackendConnection conn("ws://127.0.0.1:1", "", "", false);
  EXPECT_FALSE(conn.getVocabulary().has_value());
}
