#!/usr/bin/env python3
"""Convert the Dexory proposal markdown to a styled, self-contained HTML file.

Output is suitable for:
  - Uploading to Google Drive (Drive renders the HTML, or right-click -> "Open with
    Google Docs" converts it to an editable Google Doc).
  - Headless-Chrome PDF generation (`google-chrome --headless --print-to-pdf=...`).
"""

import sys
from pathlib import Path

import markdown

ROOT = Path(__file__).parent
IN_PATH = ROOT / "arch" / "2026-06-01-dexory-proposal.md"
OUT_PATH = ROOT / "docs" / "2026-06-01-dexory-proposal.html"

CSS = r"""
:root {
  --fg: #1d1d1f;
  --fg-muted: #4a4a4f;
  --bg: #ffffff;
  --accent: #2f5a8c;
  --rule: #d8d8db;
  --code-bg: #f5f5f7;
  --code-border: #e4e4e7;
  --table-stripe: #fafafb;
}

* { box-sizing: border-box; }

html {
  font-size: 11pt;
  -webkit-font-smoothing: antialiased;
}

body {
  font-family: -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  color: var(--fg);
  background: var(--bg);
  line-height: 1.55;
  max-width: 760px;
  margin: 2.4rem auto;
  padding: 0 1.5rem;
}

h1 {
  font-size: 1.8rem;
  font-weight: 700;
  letter-spacing: -0.01em;
  margin: 0 0 1.2rem 0;
  padding-bottom: 0.6rem;
  border-bottom: 2px solid var(--fg);
}

h2 {
  font-size: 1.25rem;
  font-weight: 700;
  margin: 2.2rem 0 0.8rem 0;
  padding-bottom: 0.35rem;
  border-bottom: 1px solid var(--rule);
  page-break-after: avoid;
  break-after: avoid;
}

h3 {
  font-size: 1.05rem;
  font-weight: 600;
  margin: 1.4rem 0 0.5rem 0;
  page-break-after: avoid;
  break-after: avoid;
}

p, ul, ol {
  margin: 0.55rem 0 0.85rem 0;
}

ul, ol { padding-left: 1.5rem; }

li { margin: 0.2rem 0; }

a {
  color: var(--accent);
  text-decoration: none;
  border-bottom: 1px solid rgba(47, 90, 140, 0.35);
}

strong { font-weight: 600; color: var(--fg); }

em { color: var(--fg-muted); }

hr {
  border: none;
  border-top: 1px solid var(--rule);
  margin: 1.8rem 0;
}

code {
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 0.88em;
  background: var(--code-bg);
  border: 1px solid var(--code-border);
  border-radius: 3px;
  padding: 0.05em 0.3em;
}

pre {
  background: var(--code-bg);
  border: 1px solid var(--code-border);
  border-radius: 4px;
  padding: 0.9rem 1rem;
  overflow-x: auto;
  line-height: 1.35;
  page-break-inside: avoid;
  break-inside: avoid;
  margin: 0.8rem 0;
}

pre code {
  background: none;
  border: none;
  padding: 0;
  font-size: 0.78rem;
}

table {
  border-collapse: collapse;
  width: 100%;
  margin: 0.8rem 0;
  font-size: 0.95em;
  page-break-inside: avoid;
  break-inside: avoid;
}

th, td {
  padding: 0.55rem 0.75rem;
  text-align: left;
  border: 1px solid var(--rule);
  vertical-align: top;
}

thead th {
  background: var(--code-bg);
  font-weight: 600;
}

tbody tr:nth-child(even) {
  background: var(--table-stripe);
}

blockquote {
  border-left: 3px solid var(--accent);
  margin: 0.8rem 0;
  padding: 0.2rem 1rem;
  color: var(--fg-muted);
}

/* Header metadata table (the first one immediately after the H1) gets
   no header row + a quieter style. */
body > table:first-of-type {
  margin-top: 0.4rem;
  font-size: 0.92em;
}
body > table:first-of-type tbody tr:nth-child(even) { background: transparent; }
body > table:first-of-type td:first-child { width: 28%; color: var(--fg-muted); }

/* Print-specific tuning. */
@page {
  size: A4;
  margin: 18mm 18mm 20mm 18mm;
}

@media print {
  html { font-size: 10.5pt; }
  body {
    max-width: none;
    margin: 0;
    padding: 0;
  }
  h2 { margin-top: 1.6rem; }
  a {
    color: inherit;
    border-bottom: none;
  }
  pre, table { page-break-inside: avoid; }
  h1, h2, h3 { page-break-after: avoid; }
}
"""

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Proposal — PlotJuggler Cloud Connector for Dexory</title>
<style>{css}</style>
</head>
<body>
{body}
</body>
</html>
"""


def main() -> int:
    md_text = IN_PATH.read_text(encoding="utf-8")
    html_body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "attr_list", "sane_lists"],
        output_format="html5",
    )
    OUT_PATH.write_text(
        HTML_TEMPLATE.format(css=CSS, body=html_body),
        encoding="utf-8",
    )
    print(f"wrote {OUT_PATH} ({OUT_PATH.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
