// Package dashboard implements the operator-facing read-only HTML admin UI
// (design spec §8.5). Same Go binary, same TCP listener, same TLS cert. Routes:
//
//	GET /                      -> 302 /dashboard/
//	GET /dashboard/            -> overview (server stats, catalog size, sessions)
//	GET /dashboard/files       -> paginated file list (reuses catalog.FilterFiles)
//	GET /dashboard/files/{id}  -> file detail (topics + both tag layers)
//	GET /dashboard/sessions    -> active sessions (registry snapshot)
//	GET /dashboard/indexer     -> catalog build-freshness (Python builder) + recent quarantine failures
//	                              (route name kept as "indexer" for URL stability; the Go
//	                              in-process indexer it originally reported on is gone, §2.6)
//	GET /static/{file}         -> pico.css (go:embed)
//
// Auth: HTTP Basic, configured via dashboard.basic_auth; the dashboard is
// DISABLED until configured (the caller only calls Register when
// config.DashboardConfig.Active() is true). /health and /metrics are independent
// and unauthenticated by default.
package dashboard

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/session"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

// Deps are the dashboard's read-only dependencies. Every datum reuses the same
// catalog.Store + session.Registry the WS handlers use (spec §8.5).
type Deps struct {
	Store         *catalog.Store
	Sessions      *session.Registry
	StartedAt     time.Time
	ServerVersion string
	Backend       string // e.g. "s3"
	Bucket        string
	BasicAuthUser string
	BasicAuthPwd  string
}

// pageTemplates holds one parsed template-set per page. Each content template
// defines "content" (they conflict in a single set), so we parse layout.html
// together with exactly one content file per page.
type pageTemplates struct {
	overview   *template.Template
	files      *template.Template
	fileDetail *template.Template
	sessions   *template.Template
	indexer    *template.Template
}

func parsePages() (*pageTemplates, error) {
	one := func(content string) (*template.Template, error) {
		return template.ParseFS(templatesFS, "templates/layout.html", "templates/"+content)
	}
	pt := &pageTemplates{}
	var err error
	if pt.overview, err = one("overview.html"); err != nil {
		return nil, err
	}
	if pt.files, err = one("files.html"); err != nil {
		return nil, err
	}
	if pt.fileDetail, err = one("file_detail.html"); err != nil {
		return nil, err
	}
	if pt.sessions, err = one("sessions.html"); err != nil {
		return nil, err
	}
	if pt.indexer, err = one("indexer.html"); err != nil {
		return nil, err
	}
	return pt, nil
}

// Register installs the dashboard + static routes on the given mux behind Basic
// auth. The caller is responsible for the "disabled until configured" gate
// (only call when credentials are present).
func Register(mux *http.ServeMux, d Deps) error {
	pt, err := parsePages()
	if err != nil {
		return fmt.Errorf("parse templates: %w", err)
	}
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return err
	}
	h := &pageHandler{deps: d, tpl: pt}

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))))
	mux.Handle("/dashboard/", basicAuth(d.BasicAuthUser, d.BasicAuthPwd, h))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Only redirect the bare root; anything else under "/" is 404 here.
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/dashboard/", http.StatusFound)
	})
	return nil
}

type pageHandler struct {
	deps Deps
	tpl  *pageTemplates
}

func (h *pageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/dashboard/" || path == "/dashboard":
		h.renderOverview(w, r)
	case path == "/dashboard/files":
		h.renderFiles(w, r)
	case strings.HasPrefix(path, "/dashboard/files/"):
		h.renderFileDetail(w, r, strings.TrimPrefix(path, "/dashboard/files/"))
	case path == "/dashboard/sessions":
		h.renderSessions(w, r)
	case path == "/dashboard/indexer":
		h.renderIndexer(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *pageHandler) base(title string) map[string]any {
	return map[string]any{"Title": title, "ServerVersion": h.deps.ServerVersion}
}

func (h *pageHandler) renderOverview(w http.ResponseWriter, r *http.Request) {
	fileCount, totalSize := h.catalogStats(r.Context())
	data := h.base("Overview")
	data["Uptime"] = time.Since(h.deps.StartedAt).Round(time.Second).String()
	data["Backend"] = h.deps.Backend
	data["Bucket"] = h.deps.Bucket
	data["FileCount"] = fileCount
	data["TotalSizeHuman"] = humanBytes(totalSize)
	data["SessionCount"] = h.deps.Sessions.ActiveCount()
	h.exec(w, h.tpl.overview, data)
}

func (h *pageHandler) renderFiles(w http.ResponseWriter, r *http.Request) {
	page := r.URL.Query().Get("page")
	files, next, err := catalog.FilterFiles(r.Context(), h.deps.Store,
		catalog.FilterArgs{Limit: 100, PageToken: page})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows := make([]map[string]any, 0, len(files))
	for _, f := range files {
		rows = append(rows, map[string]any{
			"ID":           f.ID,
			"S3Key":        f.S3Key,
			"StartHuman":   nsHuman(f.StartTimeNs),
			"EndHuman":     nsHuman(f.EndTimeNs),
			"SizeHuman":    humanBytes(f.SizeBytes),
			"MessageCount": f.MessageCount,
			"TopicCount":   f.TopicCount,
		})
	}
	data := h.base("Files")
	data["Files"] = rows
	data["Count"] = len(rows)
	data["NextPage"] = next
	h.exec(w, h.tpl.files, data)
}

func (h *pageHandler) renderFileDetail(w http.ResponseWriter, r *http.Request, idStr string) {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	rec, err := catalog.GetFile(r.Context(), h.deps.Store, id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	topics, _ := catalog.ListTopicsForFile(r.Context(), h.deps.Store, id)
	tags, _ := catalog.EffectiveTags(r.Context(), h.deps.Store, id)
	data := h.base(rec.S3Key)
	data["ID"] = rec.ID
	data["S3Key"] = rec.S3Key
	data["StartHuman"] = nsHuman(rec.StartTimeNs)
	data["EndHuman"] = nsHuman(rec.EndTimeNs)
	data["SizeHuman"] = humanBytes(rec.SizeBytes)
	data["MessageCount"] = rec.MessageCount
	data["Topics"] = topics
	data["Tags"] = tags
	h.exec(w, h.tpl.fileDetail, data)
}

func (h *pageHandler) renderSessions(w http.ResponseWriter, _ *http.Request) {
	snap := h.deps.Sessions.Snapshot()
	data := h.base("Sessions")
	data["Count"] = len(snap)
	data["Sessions"] = snap
	h.exec(w, h.tpl.sessions, data)
}

// renderIndexer serves /dashboard/indexer — retained as the catalog-freshness +
// quarantine page (route/method name kept for URL stability) now that the Go
// indexer is gone: the Python mcap_catalog builder is the sole writer, so there
// is no in-process run/duration/scanned/new/reindexed/unchanged/failed status to
// show here anymore. What remains is exactly what still applies to a read-only
// reader: the builder's build_metadata freshness snapshot (§6.5) and the
// catalog's quarantined-file list (§4.5).
func (h *pageHandler) renderIndexer(w http.ResponseWriter, r *http.Request) {
	rows, _ := h.recentFailures(r.Context())
	data := h.base("Catalog")
	data["RecentErrors"] = rows
	// Catalog-freshness panel (§6.5): the Python builder's build_metadata.
	if bi, err := catalog.GetBuildInfo(r.Context(), h.deps.Store); err == nil && bi.Present {
		data["CatalogBuild"] = map[string]string{
			"BuildID": strconv.FormatInt(bi.BuildID, 10),
			"When":    nsHuman(bi.LastBuildNs),
			"Scanned": strconv.FormatInt(bi.FilesScanned, 10),
			"Failed":  strconv.FormatInt(bi.FilesFailed, 10),
			"Outcome": bi.Outcome,
			"Version": bi.BuilderVersion,
		}
	}
	h.exec(w, h.tpl.indexer, data)
}

func (h *pageHandler) exec(w http.ResponseWriter, t *template.Template, data map[string]any) {
	if err := t.ExecuteTemplate(w, "layout.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *pageHandler) catalogStats(ctx context.Context) (int64, int64) {
	var fc, sz int64
	_ = h.deps.Store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM files`).Scan(&fc, &sz)
	return fc, sz
}

func (h *pageHandler) recentFailures(ctx context.Context) ([]map[string]string, error) {
	// catalog.RecentFailures reads catalog_failures — the Python builder's
	// quarantine table (§4.5); the legacy Go-writer indexer_failures path is
	// gone (§2.6).
	fails, err := catalog.RecentFailures(ctx, h.deps.Store, 20)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]string, 0, len(fails))
	for _, f := range fails {
		out = append(out, map[string]string{"Key": f.Key, "When": nsHuman(f.WhenNs), "Err": f.ErrText})
	}
	return out, nil
}

func nsHuman(ns int64) string {
	if ns == 0 {
		return "—"
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}

func humanBytes(n int64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
		TB = 1 << 40
	)
	switch {
	case n >= TB:
		return fmt.Sprintf("%.2f TB", float64(n)/TB)
	case n >= GB:
		return fmt.Sprintf("%.2f GB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.2f KB", float64(n)/KB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}
