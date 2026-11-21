// Package console serves the operator console's static shell: one HTML
// document, its stylesheet and its ES modules, embedded in the binary.
//
// The shell carries no data at all. Every value it shows comes from the
// Management surface under /admin/*, which keeps its own bearer credential
// gate: the page is the entry an operator opens in a browser, and a browser
// cannot attach an Authorization header to a navigation. Uploading a
// credential into the document instead would be worse — a shell served behind
// the gate could never be opened, and one with the credential baked in would
// publish it. So the shell is answered without a credential and the API beside
// it is not (04-architecture.md §8 管理 UI).
package console

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// assets is the whole console: the document and the files it references.
//
//go:embed assets
var assets embed.FS

// indexDocument is the path of the page inside the embedded tree.
const indexDocument = "assets/index.html"

// assetPrefix is the URL prefix the page's own references resolve under.
const assetPrefix = "/admin/assets/"

// contentTypes answers the served type of the console's file kinds. An unknown
// extension is refused rather than guessed: the console ships three kinds of
// file, and a fourth arriving unsniffed is a change worth noticing.
var contentTypes = map[string]string{
	".html": "text/html; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".mjs":  "text/javascript; charset=utf-8",
}

// Handler answers the console shell and its files. Mount it at GET /admin (a
// redirect to the page), GET /admin/{$} (the page) and GET /admin/assets/
// (the files); every other /admin/* route stays with the Management surface.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /admin", http.RedirectHandler("/admin/", http.StatusPermanentRedirect))
	mux.HandleFunc("GET /admin/{$}", shell)
	mux.HandleFunc("GET "+assetPrefix, asset)
	return mux
}

// shell answers the console document. It is never cached: the document names
// the revision of the files it loads, and a stale document would point at a
// module set that no longer exists after an upgrade.
func shell(w http.ResponseWriter, r *http.Request) {
	document, err := fs.ReadFile(assets, indexDocument)
	if err != nil {
		refuse(w, http.StatusInternalServerError)
		return
	}
	harden(w)
	w.Header().Set("Content-Type", contentTypes[".html"])
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(document)
}

// asset answers one file the document references. A name outside the embedded
// tree, a directory, an unknown extension and a file that is not there all
// answer 404: the handler never lists the tree and never resolves a name
// against the local filesystem.
func asset(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, assetPrefix)
	if name == "" || strings.HasSuffix(name, "/") || !fs.ValidPath(path.Clean(name)) || path.Clean(name) != name {
		refuse(w, http.StatusNotFound)
		return
	}
	kind := contentTypes[path.Ext(name)]
	if kind == "" {
		refuse(w, http.StatusNotFound)
		return
	}
	file, err := fs.ReadFile(assets, path.Join("assets", name))
	if err != nil {
		refuse(w, http.StatusNotFound)
		return
	}
	harden(w)
	w.Header().Set("Content-Type", kind)
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(file)
}

// refuse answers one bare status. The console has no error body worth
// rendering, and a status-only answer keeps the tree free of an
// http.Error-shaped call that the log-key census reads as a log call.
func refuse(w http.ResponseWriter, status int) {
	w.WriteHeader(status)
}

// harden sets the answer headers the document's own policy cannot express: a
// <meta> policy is ignored for frame-ancestors, and nosniff keeps a mistyped
// file from being interpreted as another kind.
func harden(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "+
			"base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
}
