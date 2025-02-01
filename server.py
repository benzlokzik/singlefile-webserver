import asyncio
import mimetypes
import pathlib
import logging
import re
import io
import tokenize
import token
import keyword
from urllib.parse import unquote

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


def highlight_python_code(code: str) -> str:
    """
    Very basic syntax highlighter for Python code using built-in modules.
    It wraps Python keywords, strings, numbers, and comments in span tags with classes.
    """
    result = []
    try:
        tokens = tokenize.generate_tokens(io.StringIO(code).readline)
        for tok in tokens:
            t_type, t_string, _, _, _ = tok
            # Assign a CSS class based on token type.
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
    except Exception:
        # In case of any error, fall back to unhighlighted code.
        return code
    return "".join(result)


def render_markdown(markdown_text: str) -> str:
    """
    Convert a Markdown string to HTML with basic formatting.
    In addition to headings, bold, italics, inline code, and links,
    this function processes code blocks. If a code block specifies a language
    (e.g. "```python"), then for Python code a simple highlighter is applied.
    """
    lines = markdown_text.splitlines()
    html_lines = []
    in_code_block = False
    code_block = []
    code_lang = ""

    for line in lines:
        # Detect code block start/end
        if line.startswith("```"):
            match = re.match(r"^```(\w+)?", line)
            lang = match.group(1) if match and match.group(1) else ""
            if not in_code_block:
                in_code_block = True
                code_block = []
                code_lang = lang.lower()
            else:
                in_code_block = False
                code_content = "\n".join(code_block)
                if code_lang == "python":
                    code_content = highlight_python_code(code_content)
                # Wrap in <pre><code> with an optional language class.
                html_lines.append(
                    f'<pre><code class="language-{code_lang}">{code_content}</code></pre>'
                )
            continue

        if in_code_block:
            code_block.append(line)
            continue

        # Headings: from "# " to "###### "
        header_match = re.match(r"^(#{1,6})\s+(.*)", line)
        if header_match:
            level = len(header_match.group(1))
            content = header_match.group(2)
            html_lines.append(f"<h{level}>{content}</h{level}>")
        else:
            # Inline formatting:
            # Bold: **text**
            line = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
            # Italic: *text*
            line = re.sub(r"\*(.+?)\*", r"<em>\1</em>", line)
            # Inline code: `code`
            line = re.sub(r"`(.+?)`", r"<code>\1</code>", line)
            # Links: [text](url)
            line = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', line)
            if line.strip():
                html_lines.append(f"<p>{line}</p>")
            else:
                html_lines.append("")

    converted = "\n".join(html_lines)
    # CSS styles for Markdown and code highlighting.
    style = """
    <style>
        body { font-family: sans-serif; margin: 2rem; }
        pre code {
            background-color: #f6f8fa;
            padding: 1em;
            display: block;
            overflow-x: auto;
            font-family: monospace;
        }
        span.keyword { color: #d73a49; font-weight: bold; }
        span.string { color: #032f62; }
        span.number { color: #005cc5; }
        span.comment { color: #6a737d; font-style: italic; }
    </style>
    """
    return (
        "<!DOCTYPE html>"
        "<html lang='en'>"
        "<head><meta charset='utf-8'><title>Markdown Render</title>" + style + "</head>"
        f"<body>{converted}</body></html>"
    )


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
            raise ValueError("Header size exceeds maximum allowed")
        if line == b"\r\n":  # End of headers
            break
        headers.append(line.decode("utf-8", "ignore"))
    return "".join(headers)


def parse_request(req: str) -> dict:
    """Parse HTTP request into structured data."""
    if not req:
        raise ValueError("Empty request")

    lines = req.strip().splitlines()
    if not lines:
        raise ValueError("Malformed request")

    # Parse request line.
    try:
        method, path, http_version = lines[0].split(maxsplit=2)
    except ValueError:
        raise ValueError("Invalid request line") from None

    # Normalize path.
    path = unquote(path)  # URL decode path.
    if "?" in path:  # Remove query parameters.
        path = path.split("?", 1)[0]

    # Preserve root path as "/".
    if path == "/":
        path = "/"
    else:
        path = path.lstrip("/")  # Remove leading slashes but preserve trailing.

    # Parse headers.
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

    # Prevent directory traversal.
    if not requested.is_relative_to(ROOT):
        logger.warning("Directory traversal attempt: %s", request_path)
        return None

    return requested if requested.exists() else None


def generate_directory_listing(path: pathlib.Path) -> bytes:
    """Generate HTML directory listing."""
    items = []
    current_rel = path.relative_to(ROOT)

    if path != ROOT:
        parent = path.parent.relative_to(ROOT)
        items.append(f'<li><a href="/{parent}/">.. (Parent Directory)</a></li>')

    for item in sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name)):
        rel_path = item.relative_to(ROOT)
        if item.is_dir():
            items.append(f'<li><a href="/{rel_path}/">{item.name}/</a></li>')
        else:
            items.append(
                f'<li><a href="/{rel_path}">{item.name}</a>'
                f" ({item.stat().st_size // 1024} KB)</li>"
            )

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Directory listing for /{current_rel}</title>
        <style>
            body {{ font-family: sans-serif; margin: 2rem; }}
            ul {{ list-style-type: none; padding: 0; }}
            li {{ padding: 0.5rem; }}
            a {{ color: #0366d6; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            .container {{ max-width: 800px; margin: 0 auto; }}
            .header {{ border-bottom: 1px solid #eaecef; margin-bottom: 1rem; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Directory listing for /{current_rel}</h1>
            </div>
            <ul>
                {"".join(items)}
            </ul>
        </div>
    </body>
    </html>
    """
    return html.strip().encode("utf-8")


def create_response(
    request: dict,
    content: bytes,
    is_directory: bool = False,
    override_content_type: str = None,
) -> tuple:
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
        # Determine MIME type and set charset for text files.
        mime_type, _ = mimetypes.guess_type(request["path"])
        content_type = mime_type or "application/octet-stream"
        if content_type.startswith("text/"):
            content_type += "; charset=utf-8"

    headers.update(
        {
            "Content-Type": content_type,
            "Content-Length": str(len(content)),
        }
    )

    header_lines = "\r\n".join(f"{k}: {v}" for k, v in headers.items())
    return f"{status_line}\r\n{header_lines}\r\n\r\n".encode("utf-8"), content


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handle client connection."""
    client_ip = writer.get_extra_info("peername")[0]
    logger.info("New connection from %s", client_ip)

    try:
        # Read and parse headers.
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

        # Validate request.
        if request["method"] not in ALLOWED_METHODS:
            response = b"HTTP/1.1 405 Method Not Allowed\r\nAllow: GET, HEAD\r\n\r\n"
        else:
            resolved_path = validate_path(request["path"])
            if not resolved_path:
                response = b"HTTP/1.1 404 Not Found\r\n\r\n"
            else:
                # Handle directory redirection.
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
                        ).encode("utf-8")
                    else:
                        content = generate_directory_listing(resolved_path)
                        headers, body = create_response(
                            request, content, is_directory=True
                        )
                        response = headers + (
                            b"" if request["method"] == "HEAD" else body
                        )
                else:
                    try:
                        # If the requested file is Markdown, render it to HTML.
                        if resolved_path.suffix.lower() == ".md":
                            md_text = resolved_path.read_text(encoding="utf-8")
                            rendered_html = render_markdown(md_text)
                            content = rendered_html.encode("utf-8")
                        else:
                            content = resolved_path.read_bytes()
                    except PermissionError as e:
                        logger.warning("Permission denied: %s", e)
                        response = b"HTTP/1.1 403 Forbidden\r\n\r\n"
                    except Exception as e:
                        logger.error("File read error: %s", e)
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

        # Send response.
        writer.write(response)
        await writer.drain()

    except Exception as e:
        logger.error("Error handling request: %s", e, exc_info=True)
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info("Connection closed: %s", client_ip)


async def run_server(host: str = "0.0.0.0", port: int = 9000):
    """Start the async HTTP server."""
    server = await asyncio.start_server(handle_client, host, port)
    async with server:
        logger.info("Server started on http://%s:%d", host, port)
        await server.serve_forever()


if __name__ == "__main__":
    mimetypes.init()
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info("Server stopped")
