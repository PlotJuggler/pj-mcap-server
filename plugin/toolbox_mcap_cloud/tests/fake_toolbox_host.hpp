// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// FakeToolboxHost — a PJ_toolbox_host_t recorder that, unlike the SDK's
// ToolboxTestStore, records the TOPIC NAME per appended row (so per-topic
// row counts + field values are assertable). Modeled on the SDK
// ParserWriteRecorder extraction.
//
// Split out from ros_decode_test_support.hpp (which also carried rosx
// introspection types) so tests that only need the fake host — e.g.
// session_resume_live_test — can include this without pulling in rosx.
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <pj_base/plugin_data_api.h>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>

namespace mcap_cloud::testsupport {

// One captured field (name + type + value), reusing the recorder shape.
struct CapturedField {
  std::string name;
  PJ::PrimitiveType type = PJ::PrimitiveType::kUnspecified;
  bool is_null = false;
  double numeric = 0.0;
  bool bool_value = false;
  std::string string_value;
};

struct CapturedRow {
  std::uint32_t topic_id = 0;  // FakeToolboxHost topic handle id
  std::string topic_name;
  std::int64_t timestamp = 0;
  std::vector<CapturedField> fields;
};

inline void extractValue(const PJ_scalar_value_t& v, CapturedField& out) {
  out.type = static_cast<PJ::PrimitiveType>(v.type);
  switch (v.type) {
    case PJ_PRIMITIVE_TYPE_FLOAT64: out.numeric = v.data.as_float64; break;
    case PJ_PRIMITIVE_TYPE_FLOAT32: out.numeric = static_cast<double>(v.data.as_float32); break;
    case PJ_PRIMITIVE_TYPE_INT8: out.numeric = static_cast<double>(v.data.as_int8); break;
    case PJ_PRIMITIVE_TYPE_INT16: out.numeric = static_cast<double>(v.data.as_int16); break;
    case PJ_PRIMITIVE_TYPE_INT32: out.numeric = static_cast<double>(v.data.as_int32); break;
    case PJ_PRIMITIVE_TYPE_INT64: out.numeric = static_cast<double>(v.data.as_int64); break;
    case PJ_PRIMITIVE_TYPE_UINT8: out.numeric = static_cast<double>(v.data.as_uint8); break;
    case PJ_PRIMITIVE_TYPE_UINT16: out.numeric = static_cast<double>(v.data.as_uint16); break;
    case PJ_PRIMITIVE_TYPE_UINT32: out.numeric = static_cast<double>(v.data.as_uint32); break;
    case PJ_PRIMITIVE_TYPE_UINT64: out.numeric = static_cast<double>(v.data.as_uint64); break;
    case PJ_PRIMITIVE_TYPE_BOOL:
      out.bool_value = (v.data.as_bool != 0);
      out.numeric = out.bool_value ? 1.0 : 0.0;
      break;
    case PJ_PRIMITIVE_TYPE_STRING:
      if (v.data.as_string.data != nullptr) {
        out.string_value.assign(v.data.as_string.data, v.data.as_string.size);
      }
      break;
    default: break;
  }
}

// A toolbox host recorder that tracks the topic name per row. ensure_topic
// allocates a fresh handle id and remembers its name; append_record captures
// the row under that handle's name.
class FakeToolboxHost {
 public:
  FakeToolboxHost() = default;
  FakeToolboxHost(const FakeToolboxHost&) = delete;
  FakeToolboxHost& operator=(const FakeToolboxHost&) = delete;

  PJ_toolbox_host_t makeHost() {
    PJ_toolbox_host_vtable_t* vt = &vtable_;
    vt->abi_version = PJ_PLUGIN_DATA_API_VERSION;
    vt->struct_size = sizeof(PJ_toolbox_host_vtable_t);
    vt->create_data_source = &FakeToolboxHost::tCreateDataSource;
    vt->ensure_topic = &FakeToolboxHost::tEnsureTopic;
    vt->ensure_field = &FakeToolboxHost::tEnsureField;
    vt->append_record = &FakeToolboxHost::tAppendRecord;
    vt->append_bound_record = &FakeToolboxHost::tAppendBoundRecord;
    vt->append_arrow_stream = nullptr;
    vt->acquire_catalog_snapshot = nullptr;
    vt->read_series_arrow = nullptr;
    vt->register_object_topic = nullptr;
    vt->push_owned_object = nullptr;
    return PJ_toolbox_host_t{.ctx = this, .vtable = vt};
  }

  PJ::sdk::ToolboxHostView view() { return PJ::sdk::ToolboxHostView(makeHost()); }

  const std::vector<CapturedRow>& rows() const { return rows_; }
  int createDataSourceCalls() const { return create_calls_; }

  // Row count for a topic by name.
  std::size_t rowCount(const std::string& topic) const {
    std::size_t n = 0;
    for (const auto& r : rows_) {
      if (r.topic_name == topic) {
        ++n;
      }
    }
    return n;
  }

  // Force ensure_topic to fail for a given name (tests the bind error path).
  void failTopic(std::string name) { fail_topic_ = std::move(name); }

 private:
  static bool tCreateDataSource(void* ctx, PJ_string_view_t, PJ_data_source_handle_t* out, PJ_error_t*) noexcept {
    auto* self = static_cast<FakeToolboxHost*>(ctx);
    ++self->create_calls_;
    *out = PJ_data_source_handle_t{1};
    return true;
  }
  static bool tEnsureTopic(
      void* ctx, PJ_data_source_handle_t, PJ_string_view_t name, PJ_topic_handle_t* out, PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeToolboxHost*>(ctx);
    std::string n(name.data == nullptr ? "" : name.data, name.size);
    if (!self->fail_topic_.empty() && n == self->fail_topic_) {
      if (err != nullptr) {
        PJ::sdk::fillError(err, 1, "test", "forced ensureTopic failure");
      }
      return false;
    }
    auto it = self->topic_id_by_name_.find(n);
    std::uint32_t id;
    if (it == self->topic_id_by_name_.end()) {
      id = self->next_topic_id_++;
      self->topic_id_by_name_.emplace(n, id);
      self->name_by_topic_id_.emplace(id, n);
    } else {
      id = it->second;
    }
    *out = PJ_topic_handle_t{id};
    return true;
  }
  static bool tEnsureField(
      void*, PJ_topic_handle_t topic, PJ_string_view_t, PJ_primitive_type_t, PJ_field_handle_t* out,
      PJ_error_t*) noexcept {
    *out = PJ_field_handle_t{topic, 0};
    return true;
  }
  static bool tAppendRecord(
      void* ctx, PJ_topic_handle_t topic, int64_t ts, const PJ_named_field_value_t* fields, uint64_t count,
      PJ_error_t*) noexcept {
    auto* self = static_cast<FakeToolboxHost*>(ctx);
    CapturedRow row;
    row.topic_id = topic.id;
    if (auto it = self->name_by_topic_id_.find(topic.id); it != self->name_by_topic_id_.end()) {
      row.topic_name = it->second;
    }
    row.timestamp = ts;
    row.fields.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
      CapturedField f;
      if (fields[i].name.data != nullptr) {
        f.name.assign(fields[i].name.data, fields[i].name.size);
      }
      f.is_null = fields[i].is_null;
      extractValue(fields[i].value, f);
      row.fields.push_back(std::move(f));
    }
    self->rows_.push_back(std::move(row));
    return true;
  }
  static bool tAppendBoundRecord(
      void*, PJ_topic_handle_t, int64_t, const PJ_bound_field_value_t*, uint64_t, PJ_error_t*) noexcept {
    return true;
  }

  PJ_toolbox_host_vtable_t vtable_{};
  std::vector<CapturedRow> rows_;
  std::unordered_map<std::string, std::uint32_t> topic_id_by_name_;
  std::unordered_map<std::uint32_t, std::string> name_by_topic_id_;
  std::uint32_t next_topic_id_ = 100;
  int create_calls_ = 0;
  std::string fail_topic_;
};

}  // namespace mcap_cloud::testsupport
