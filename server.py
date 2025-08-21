from __future__ import annotations

import asyncio
import io
from datetime import datetime
import keyword
import logging
import mimetypes
import pathlib
import re
import token
import tokenize
from typing import TYPE_CHECKING
from urllib.parse import unquote

if TYPE_CHECKING:
    from collections.abc import Iterable

# Configure logging
logging.basicConfig(
    format="%(asctime)s - server.py:%(lineno)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ROOT = pathlib.Path(__file__).parent.resolve()
ALLOWED_METHODS = {"GET", "HEAD"}
BUFFER_SIZE = 1024
MAX_HEADER_SIZE = 8192  # 8KB
KB = 1024


def format_size(size: int) -> str:
    if size < KB:
        return f"{size} B"
    elif size < KB**2:
        return f"{size / KB:.2f} KB"
    else:
        return f"{size / KB**2:.2f} MB"


def highlight_python_code(code: str) -> str:
    """Highlight basic Python syntax while preserving original spacing.

    Uses token position info (start_row, start_col, end_row, end_col)
    to reconstruct the original layout.
    """
    result = []
    tokens = list(tokenize.generate_tokens(io.StringIO(code).readline))

    # Track the previous token's end position so we can
    # insert the appropriate number of spaces/newlines.
    prev_row, prev_col = 1, 0

    for t_type, t_string, (start_row, start_col), (end_row, end_col), _ in tokens:
        # Insert newlines if we jumped to a new row.
        while prev_row < start_row:
            result.append("\n")
            prev_row += 1
            prev_col = 0

        # Insert spaces if we jumped columns in the same row.
        while prev_col < start_col:
            result.append(" ")
            prev_col += 1

        # Highlight based on token type.
        if t_type == token.STRING:
            result.append(f'<span class="string">{t_string}</span>')
        elif t_type == token.NUMBER:
            result.append(f'<span class="number">{t_string}</span>')
        elif t_type == token.COMMENT:
            result.append(f'<span class="comment">{t_string}</span>')
        elif t_type == token.NAME and t_string in keyword.kwlist:
            result.append(f'<span class="keyword">{t_string}</span>')
        else:
            result.append(t_string)

        # Update previous token position to this token's end.
        prev_row, prev_col = end_row, end_col

    return "".join(result)


def render_markdown(markdown_text: str) -> str:
    """Convert a Markdown string to HTML using only built-in libraries.

    Features supported:
      - Headings (using 1-6 '#' characters)
      - Bold/Italic (**text**, *text*)
      - Inline code (`code`)
      - Links ([text](url))
      - Reference-style links/images: [text][id], ![alt][id] with [id]: url
      - Images ![alt](src)
      - Image-as-link: [![alt](src)](href)
      - Lists (- ), blockquotes (> ), code fences ```lang (python gets basic highlighting)
    """
    # Collect reference-style link definitions
    ref_links: dict[str, str] = {}
    lines = markdown_text.splitlines()
    cleaned_lines = []
    for line in lines:
        m = re.match(r'^\s*\[([^\]]+)\]:\s*(\S+)', line)
        if m:
            ref_links[m.group(1).strip()] = m.group(2).strip()
        else:
            cleaned_lines.append(line)
    lines = cleaned_lines

    html_lines = []
    in_code_block = False
    in_list = False
    code_block: list[str] = []
    list_buffer: list[str] = []
    code_lang = ""

    for line in lines:
        # Code block start/end
        if line.startswith("```"):
            match = re.match(r"^```(\w+)?", line)
            lang = match.group(1).lower() if match and match.group(1) else ""
            if not in_code_block:
                in_code_block = True
                code_block = []
                code_lang = lang
            else:
                in_code_block = False
                code_content = "\n".join(code_block)
                if code_lang == "python":
                    code_content = highlight_python_code(code_content)
                html_lines.append(
                    f'<pre><code class="language-{code_lang}">{code_content}</code></pre>'
                )
            continue

        if in_code_block:
            code_block.append(line)
            continue

        # Horizontal rule
        if re.match(r"^\s*(\*|-|_){3,}\s*$", line):
            html_lines.append("<hr/>")
            continue

        # Headings
        header_match = re.match(r"^(#{1,6})\s+(.*)", line)
        if header_match:
            level = len(header_match.group(1))
            content = header_match.group(2)
            html_lines.append(f"<h{level}>{content}</h{level}>")
            continue

        # Unordered lists
        if line.startswith("- "):
            if not in_list:
                in_list = True
                list_buffer = []
            list_item = line[2:].strip()
            list_buffer.append(f"<li>{list_item}</li>")
            continue
        if in_list:
            html_lines.append("<ul>" + "".join(list_buffer) + "</ul>")
            in_list = False
            list_buffer = []

        # Blockquotes
        if line.startswith("> "):
            html_lines.append(f"<blockquote>{line[2:].strip()}</blockquote>")
            continue

        # Inline transforms — order matters
        # Code
        line = re.sub(r"`(.+?)`", r"<code>\1</code>", line)

        # Image-as-link: [![alt](src)](href)
        line = re.sub(
            r"\[\s*!\[([^\]]*?)\]\(([^)]+)\)\s*\]\(([^)]+)\)",
            r'<a href="\3"><img alt="\1" src="\2" /></a>',
            line,
        )

        # Reference-style images/links
        line = re.sub(
            r"!\[([^\]]*?)\]\[([^\]]+)\]",
            lambda m: f'<img alt="{m.group(1)}" src="{ref_links.get(m.group(2), m.group(2))}" />',
            line,
        )
        line = re.sub(
            r"\[([^\]]+)\]\[([^\]]+)\]",
            lambda m: f'<a href="{ref_links.get(m.group(2), m.group(2))}">{m.group(1)}</a>',
            line,
        )

        # Inline images
        line = re.sub(r"!\[([^\]]*?)\]\(([^)]+)\)", r'<img alt="\1" src="\2" />', line)

        # Inline links
        line = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', line)

        # Emphasis
        line = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
        line = re.sub(r"\*(.+?)\*", r"<em>\1</em>", line)
        line = re.sub(r"`(.+?)`", r"<code>\1</code>", line)
        line = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', line)

        if line.strip():
            html_lines.append(f"<p>{line}</p>")
        else:
            html_lines.append("")

    # Flush any remaining list items
    if in_list:
        html_lines.append("<ul>" + "".join(list_buffer) + "</ul>")

    converted = "\n".join(html_lines)

    body = f"""
    <div class="card">
      <h2>Markdown</h2>
      <div style="padding:16px">{converted}</div>
    </div>
    """

    # breadcrumbs for the file path are drawn on the directory listing side
    # here we leave it empty but the header remains the same
    return render_page("Markdown Render", body)


async def read_headers(reader: asyncio.StreamReader) -> str:
    """Read until end of HTTP headers with size limitation."""
    headers = []
    total_size = 0
    while True:
        line = await reader.readline()
        if not line:
            break
        total_size += len(line)
        if total_size > MAX_HEADER_SIZE:
            msg = "Header size exceeds maximum allowed"
            raise ValueError(msg)
        if line == b"\r\n":  # End of headers
            break
        headers.append(line.decode("utf-8", "ignore"))
    return "".join(headers)


def parse_request(req: str) -> dict:
    """Parse HTTP request into structured data."""
    if not req:
        msg = "Empty request"
        raise ValueError(msg)

    lines = req.strip().splitlines()
    if not lines:
        msg = "Malformed request"
        raise ValueError(msg)

    # Parse request line
    try:
        method, path, http_version = lines[0].split(maxsplit=2)
    except ValueError:
        msg = "Invalid request line"
        raise ValueError(msg) from None

    # Normalize path
    path = unquote(path)  # URL decode path
    if "?" in path:  # Remove query parameters
        path = path.split("?", 1)[0]

    # Preserve root path as "/"
    path = (
        path if path == "/" else path.lstrip("/")
    )  # Remove leading slashes but preserve trailing

    # Parse headers
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        try:
            key, value = line.split(": ", 1)
            headers[key.strip().title()] = value.strip()
        except ValueError:
            logger.warning("Invalid header line: %s", line)

    return {
        "method": method.upper(),
        "path": path,
        "version": http_version,
        "headers": headers,
    }


def validate_path(request_path: str) -> pathlib.Path:
    """Validate and resolve requested path against root directory."""
    try:
        requested = ROOT.joinpath(request_path.lstrip("/")).resolve()
    except Exception as e:
        logger.warning("Path resolution error: %s", e)
        return None

    # Prevent directory traversal
    if not requested.is_relative_to(ROOT):
        logger.warning("Directory traversal attempt: %s", request_path)
        return None

    return requested if requested.exists() else None


def generate_directory_listing(path: pathlib.Path) -> bytes:
    rel = path.relative_to(ROOT) if path != ROOT else pathlib.Path("")

    # Build rows
    rows = []

    # Parent always on top (separate class + excluded from sorting/filter)
    if path != ROOT:
        parent_rel = path.parent.relative_to(ROOT)
        rows.append(f"""
          <tr class="parent-row" data-name=".." data-size="0" data-ts="{int(path.stat().st_mtime)}" data-isdir="1" data-parent="1">
            <td class="name-col">
              <span class="icon"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M10 4l2 2h8a2 2 0 012 2v9a2 2 0 01-2 2H4a2 2 0 01-2-2V6a2 2 0 012-2h6z"/></svg></span>
              <a href="/{parent_rel}/">..</a>
              <span class="meta">Parent</span>
            </td>
            <td class="meta">—</td>
            <td class="meta">—</td>
          </tr>
        """)

    for item in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
        is_dir = item.is_dir()
        rel_item = item.relative_to(ROOT)
        href = f"/{rel_item}/" if is_dir else f"/{rel_item}"
        size = item.stat().st_size if not is_dir else 0
        mtime = int(item.stat().st_mtime)
        size_str = "—" if is_dir else format_size(size)
        mod_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        icon = ("""
          <span class="icon"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M10 4l2 2h8a2 2 0 012 2v9a2 2 0 01-2 2H4a2 2 0 01-2-2V6a2 2 0 012-2h6z"/></svg></span>
        """ if is_dir else """
          <span class="icon"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8l-6-6zM14 3.5L18.5 8H14V3.5z"/></svg></span>
        """)
        rows.append(f"""
          <tr data-name="{item.name}" data-size="{size}" data-ts="{mtime}" data-isdir="{1 if is_dir else 0}">
            <td class="name-col">{icon}<a class="file-link" href="{href}">{item.name}{'/' if is_dir else ''}</a></td>
            <td class="meta">{size_str}</td>
            <td class="meta">{mod_str}</td>
          </tr>
        """)

    table = f"""
    <div class="main">
      <div class="card">
        <h2>Contents</h2>
        <div class="table-wrap">
          <table id="files">
            <thead>
              <tr>
                <th data-key="name">Name</th>
                <th data-key="size">Size</th>
                <th data-key="ts">Modified</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows)}
            </tbody>
          </table>
        </div>
        <div style="padding:10px 12px" class="meta"><span id="count"></span></div>
      </div>

      <div class="card preview">
        <h2>Preview</h2>
        <div class="empty" id="emptyHint">Select a file to preview</div>
        <div id="mediaHost" class="media-wrap" style="display:none"></div>
        <iframe id="pv" style="display:none"></iframe>
      </div>
    </div>
    """

    # JS: sorting, filter, preview
    js = """
(function(){
  const table = document.getElementById('files');
  const tbody = table.querySelector('tbody');
  const parentRow = tbody.querySelector('tr.parent-row');
  const rows = Array.from(tbody.querySelectorAll('tr:not(.parent-row)'));
  const count = document.getElementById('count');
  const q = document.getElementById('q');

  function updateCount(){
    const visible = rows.filter(r => r.style.display !== 'none').length;
    count.textContent = `Items: ${visible}`;
  }
  updateCount();

  // Sorting (folders first; parent row pinned on top)
  let sortKey = 'name'; let sortDir = 'asc';
  function sortBy(key){
    if (sortKey === key) sortDir = (sortDir === 'asc' ? 'desc' : 'asc');
    else { sortKey = key; sortDir = 'asc'; }
    const m = sortDir === 'asc' ? 1 : -1;
    rows.sort((a,b)=>{
      const ad = +a.dataset.isdir, bd = +b.dataset.isdir;
      if (ad !== bd) return bd - ad; // directories first
      if (key === 'name') return a.dataset.name.localeCompare(b.dataset.name) * m;
      if (key === 'size') return (Number(a.dataset.size) - Number(b.dataset.size)) * m;
      return (Number(a.dataset.ts) - Number(b.dataset.ts)) * m;
    });
    // Re-append with parent row first
    if (parentRow) tbody.appendChild(parentRow);
    rows.forEach(r => tbody.appendChild(r));
  }
  table.querySelectorAll('thead th').forEach(th=>{
    th.addEventListener('click', ()=> sortBy(th.dataset.key));
  });
  sortBy('name');

  // Filter (parent row remains visible)
  q.addEventListener('input', ()=>{
    const val = q.value.toLowerCase().trim();
    rows.forEach(r=>{
      const name = r.dataset.name.toLowerCase();
      r.style.display = name.includes(val) ? '' : 'none';
    });
    updateCount();
  });

  // Preview
  const iframe = document.getElementById('pv');
  const media = document.getElementById('mediaHost');
  const empty = document.getElementById('emptyHint');
  const imgExt = ['png','jpg','jpeg','gif','webp','svg','bmp','avif'];
  const audExt = ['mp3','wav','ogg','m4a','flac','aac','opus'];
  const vidExt = ['mp4','webm','ogv','mov','mkv'];
  function preview(href){
    if (href.endsWith('/')) { window.location.href = href; return; }
    empty.style.display = 'none';
    media.style.display = 'none';
    iframe.style.display = 'none';
    media.innerHTML = '';
    const ext = href.split('.').pop().toLowerCase();

    if (imgExt.includes(ext)) {
      media.innerHTML = `<img src="${href}" alt="">`;
      media.style.display = 'block';
    } else if (audExt.includes(ext)) {
      media.innerHTML = `<audio controls preload="metadata" src="${href}" style="width:100%"></audio>`;
      media.style.display = 'block';
    } else if (vidExt.includes(ext)) {
      media.innerHTML = `<video controls preload="metadata" src="${href}" style="width:100%;background:transparent"></video>`;
      media.style.display = 'block';
    } else {
      iframe.src = href; // HTML/MD/PDF/text etc.
      iframe.style.display = 'block';
    }
  }

  tbody.addEventListener('click', (e)=>{
    const a = e.target.closest('a.file-link');
    if (!a) return;
    if (e.button === 0 && !e.metaKey && !e.ctrlKey) {
      e.preventDefault();
      preview(a.getAttribute('href'));
    }
  });

  // Breadcrumbs (client-side build)
  const bc = document.getElementById('breadcrumbs');
  const parts = window.location.pathname.split('/').filter(Boolean);
  let accum = '';
  const els = ['<a href="/">/</a>'];
  parts.forEach((p)=>{
    accum += '/' + p;
    els.push('<span class="sep">›</span><a href="'+accum+'/">'+p+'</a>');
  });
  bc.innerHTML = els.join('');
})();
"""
    body = table
    html = render_page(f"Directory listing for /{rel}", body, extra_js=js)
    return html.encode("utf-8")


def render_page(title: str, body_html: str, *, extra_css: str = "", extra_js: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="en" data-theme="">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{title}</title>
<style>
:root {{
  --bg: #fafafa;
  --fg: #212121;
  --muted: #616161;
  --card: #ffffff;
  --border: #e0e0e0;
  --accent: #3f51b5;
  --accent-2: #1e88e5;
  --code-bg: #ffffff;
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --bg: #0e0f12;
    --fg: #e8e8e8;
    --muted: #9aa0a6;
    --card: #16181d;
    --border: #2b2f36;
    --accent: #8ab4f8;
    --accent-2: #8ab4f8;
    --code-bg: #0f1115;
  }}
}}
:root[data-theme="dark"] {{
  --bg: #0e0f12;
  --fg: #e8e8e8;
  --muted: #9aa0a6;
  --card: #16181d;
  --border: #2b2f36;
  --accent: #8ab4f8;
  --accent-2: #8ab4f8;
  --code-bg: #0f1115;
}}

* {{ box-sizing: border-box; }}
html, body {{ height: 100%; }}
body {{
  margin: 0; padding: 0;
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
  background: var(--bg); color: var(--fg);
}}
a {{ color: var(--accent-2); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}

.container {{ max-width: 1100px; margin: 0 auto; padding: 24px 16px; }}

.header {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 16px; }}
.breadcrumbs a {{ color: var(--fg); }}
.breadcrumbs .sep {{ margin: 0 6px; color: var(--muted); }}
.controls {{ display: flex; gap: 8px; align-items: center; }}
input[type="search"] {{
  background: var(--card); color: var(--fg); border: 1px solid var(--border);
  padding: 8px 10px; border-radius: 8px; min-width: 220px;
}}
.toggle {{
  background: var(--card); color: var(--fg); border: 1px solid var(--border);
  padding: 8px 10px; border-radius: 8px; cursor: pointer;
}}

.main {{ display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; }}
@media (max-width: 900px) {{ .main {{ grid-template-columns: 1fr; }} }}

.card {{
  background: var(--card); border: 1px solid var(--border); border-radius: 12px;
  overflow: hidden;
}}
.card h2 {{ margin: 0; font-size: 16px; padding: 12px 14px; border-bottom: 1px solid var(--border); }}

.table-wrap {{ overflow: auto; }}
table {{ width: 100%; border-collapse: collapse; }}
thead th {{
  text-align: left; font-weight: 600; font-size: 14px; color: var(--muted);
  padding: 10px 12px; border-bottom: 1px solid var(--border); cursor: pointer; white-space: nowrap;
}}
tbody td {{ padding: 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }}
tbody tr:hover {{ background: color-mix(in oklab, var(--card) 80%, var(--accent) 10%); }}

.name-col {{ display: flex; align-items: center; gap: 10px; }}
.icon {{ width: 18px; height: 18px; display: inline-block; vertical-align: middle; opacity: 0.9; }}
.icon svg {{ width: 18px; height: 18px; }}
.meta {{ color: var(--muted); font-size: 12px; }}

.preview {{ min-height: 360px; }}
.preview iframe {{ display:block; width:100%; height: min(70vh, 720px); border: 0; background: var(--card); }}
.preview .empty {{ color: var(--muted); display: grid; place-items: center; border-top: 1px solid var(--border); min-height: 120px; }}
.preview .media-wrap {{ padding: 12px; }}
.preview .media-wrap img {{ max-width: 100%; height: auto; display: block; }}
.preview .media-wrap video {{ width: 100%; height: auto; display: block; background: transparent; }}
.preview .media-wrap audio {{ width: 100%; display: block; }}

pre code {{
  background: var(--code-bg);
  padding: 1em; display: block; overflow-x: auto;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Roboto Mono", monospace;
  border-radius: 8px; border: 1px solid var(--border); margin: 1em 0;
}}

/* Token styles used by highlight_python_code */
span.keyword {{ color: #d81b60; font-weight: 600; }}
span.string  {{ color: #388e3c; }}
span.number  {{ color: #f57c00; }}
span.comment {{ color: #9aa0a6; font-style: italic; }}

hr {{ border: 0; border-top: 1px solid var(--border); margin: 16px 0; }}

{extra_css}
</style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="breadcrumbs" id="breadcrumbs"></div>
      <div class="controls">
        <input type="search" id="q" placeholder="Search (/)">
        <button class="toggle" id="themeToggle" title="Toggle theme">🌓</button>
      </div>
    </div>
    {body_html}
  </div>
<script>
(function() {{
  // Theme toggle
  const html = document.documentElement;
  const key = "sfws-theme";
  const saved = localStorage.getItem(key);
  if (saved) html.setAttribute("data-theme", saved);
  document.getElementById("themeToggle").addEventListener("click", () => {{
    const cur = html.getAttribute("data-theme");
    const next = cur === "dark" ? "" : "dark";
    if (next) html.setAttribute("data-theme", next); else html.removeAttribute("data-theme");
    localStorage.setItem(key, next);
  }});

  // Focus search with /
  const q = document.getElementById("q");
  window.addEventListener("keydown", (e) => {{
    if (e.key === "/" && !e.metaKey && !e.ctrlKey && document.activeElement !== q) {{
      e.preventDefault(); q.focus();
    }}
  }});
}})();
{extra_js}
</script>
</body>
</html>"""


def create_response(
    request: dict,
    content: bytes,
    *,
    is_directory: bool = False,
    override_content_type: str | None = None,
) -> tuple[bytes, bytes]:
    """Create HTTP response with headers."""
    logger.debug("Request: %s", request)
    status_line = f"{request['version']} 200 OK"
    headers = {
        "Server": "AsyncFileServer/1.0",
        "Connection": "close",
        "X-Content-Type-Options": "nosniff",
    }

    if override_content_type:
        content_type = override_content_type
    elif is_directory:
        content_type = "text/html; charset=utf-8"
    else:
        # Determine MIME type and set charset for text files
        mime_type, _ = mimetypes.guess_type(request["path"])
        content_type = mime_type or "application/octet-stream"
        if content_type.startswith("text/"):
            content_type += "; charset=utf-8"

    headers.update(
        {
            "Content-Type": content_type,
            "Content-Length": str(len(content)),
        },
    )

    header_lines = "\r\n".join(f"{k}: {v}" for k, v in headers.items())
    return f"{status_line}\r\n{header_lines}\r\n\r\n".encode(), content


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """Handle client connection."""
    client_ip = writer.get_extra_info("peername")[0]
    logger.info("New connection from %s", client_ip)

    try:
        # Read and parse headers
        try:
            raw_headers = await read_headers(reader)
        except ValueError as e:
            logger.warning("Header size exceeded: %s", e)
            response = b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n"
            writer.write(response)
            return

        if not raw_headers:
            return

        try:
            request = parse_request(raw_headers)
        except ValueError as e:
            logger.warning("Invalid request: %s", e)
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return

        logger.info("%s %s", request["method"], request["path"])

        # Validate request
        if request["method"] not in ALLOWED_METHODS:
            response = b"HTTP/1.1 405 Method Not Allowed\r\nAllow: GET, HEAD\r\n\r\n"
        else:
            resolved_path = validate_path(request["path"])
            if not resolved_path:
                response = b"HTTP/1.1 404 Not Found\r\n\r\n"
            else:
                # Handle directory redirection
                is_directory = resolved_path.is_dir()
                request_path = request["path"]

                if is_directory:
                    # Ensure directory paths end with /
                    if not request_path.endswith("/"):
                        redirect_path = (
                            f"/{request_path}/" if request_path != "/" else "/"
                        )
                        response = (
                            f"{request['version']} 301 Moved Permanently\r\n"
                            f"Location: {redirect_path}\r\n"
                            "Connection: close\r\n\r\n"
                        ).encode()
                    else:
                        content = generate_directory_listing(resolved_path)
                        headers, body = create_response(
                            request,
                            content,
                            is_directory=True,
                        )
                        response = headers + (
                            b"" if request["method"] == "HEAD" else body
                        )
                else:
                    try:
                        # If the requested file is Markdown, render it to HTML
                        if resolved_path.suffix.lower() == ".md":
                            md_text = resolved_path.read_text(encoding="utf-8")
                            rendered_html = render_markdown(md_text)
                            content = rendered_html.encode("utf-8")
                        else:
                            content = resolved_path.read_bytes()
                    except PermissionError as e:
                        logger.warning("Permission denied: %s", e)
                        response = b"HTTP/1.1 403 Forbidden\r\n\r\n"
                    except Exception:
                        logger.exception("File read error")
                        response = b"HTTP/1.1 500 Internal Server Error\r\n\r\n"
                    else:
                        if resolved_path.suffix.lower() == ".md":
                            headers, body = create_response(
                                request,
                                content,
                                override_content_type="text/html; charset=utf-8",
                            )
                        else:
                            headers, body = create_response(request, content)
                        response = headers + (
                            b"" if request["method"] == "HEAD" else body
                        )

        # Send response
        writer.write(response)
        await writer.drain()

    except Exception:
        logger.exception("Error handling request")
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info("Connection closed: %s", client_ip)


async def ping_server(host: str, port: int) -> None:
    """Test the server availability by sending a simple HTTP GET request.
    Raises OSError if the connection fails or no data is received.
    """
    try:
        reader, writer = await asyncio.open_connection(host, port)
        # Sending a minimal HTTP request
        writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await reader.read(1024)
        if not response:
            msg = f"No response received from {host}:{port}"
            raise OSError(msg)
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        msg = f"Ping failed for {host}:{port} - {e}"
        raise OSError(msg) from e


async def test_server_availability(port: int) -> None:
    """Test the server availability on both 127.0.0.1 and 0.0.0.0.
    Raises OSError if any of the pings fails.
    """
    for test_host in ("127.0.0.1", "0.0.0.0"):
        await ping_server(test_host, port)


async def attempt_server(port: int, host: str) -> tuple[int, asyncio.AbstractServer]:
    """Try to bind the server on the given port, perform the ping tests, and return the port and server.
    Raises OSError if binding or ping test fails.
    """
    try:
        server = await asyncio.start_server(handle_client, host, port)
    except OSError as e:
        msg = f"Port {port} is not available: {e}"
        raise OSError(msg)

    # Test server availability on both 127.0.0.1 and 0.0.0.0.
    try:
        await test_server_availability(port)
    except OSError as e:
        server.close()
        await server.wait_closed()
        msg = f"Ping test failed on port {port}: {e}"
        raise OSError(msg)

    return port, server


async def run_server_on_available_port(
    host: str = "0.0.0.0",
    ports: Iterable[int] = (9000,),
) -> None:
    """Attempt to start servers on multiple ports concurrently.
    Use the first one that passes the ping tests and cancel the rest.
    """
    tasks = [asyncio.create_task(attempt_server(port, host)) for port in ports]

    for completed in asyncio.as_completed(tasks):
        try:
            port, server = await completed
            logger.info(
                "Server started and passed ping tests on http://%s:%s",
                host,
                port,
            )
            # Cancel any other pending tasks.
            for task in tasks:
                if not task.done():
                    task.cancel()
            # Start serving on the successful server.
            async with server:
                await server.serve_forever()
            return
        except Exception as e:
            logger.warning("Attempt failed: %s", e)

    logger.error("No available ports found that passed ping tests. Exiting.")


if __name__ == "__main__":
    mimetypes.init()
    try:
        asyncio.run(
            run_server_on_available_port(
                ports=(
                    9000,
                    9001,
                    9002,
                    9003,
                    9004,
                    9005,
                ),
            ),
        )
    except KeyboardInterrupt:
        logger.info("Server stopped")
