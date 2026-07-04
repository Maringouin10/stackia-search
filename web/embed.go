// Package web embeds the single-page search UI and the admin dashboard into
// the binary via go:embed, so deployment is a single self-contained executable
// — no static-asset directory to ship to the Pi.
package web

import "embed"

//go:embed static/*.html
var FS embed.FS
