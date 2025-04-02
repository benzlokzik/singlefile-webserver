from __future__ import annotations

import asyncio
import io
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
      - Bold (**text**) and italics (*text*)
      - Inline code (`code`)
      - Links ([text](url))
      - Unordered lists (lines starting with "- ")
      - Blockquotes (lines starting with "> ")
      - Code blocks (```), with basic Python syntax highlighting if language is 'python'
    """
    lines = markdown_text.splitlines()
    html_lines = []
    in_code_block = False
    in_list = False
    code_block = []
    list_buffer = []
    code_lang = ""

    for line in lines:
        # Handle code block start/end.
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
                    f'<pre><code class="language-{code_lang}">{code_content}</code></pre>',
                )
            continue

        if in_code_block:
            code_block.append(line)
            continue

        # Detect horizontal rule (thematic break): --- *** ___ etc.
        hr_match = re.match(r"^\s*(\*|-|_){3,}\s*$", line)
        if hr_match:
            html_lines.append("<hr/>")
            continue

        # Handle headings.
        header_match = re.match(r"^(#{1,6})\s+(.*)", line)
        if header_match:
            level = len(header_match.group(1))
            content = header_match.group(2)
            html_lines.append(f"<h{level}>{content}</h{level}>")
            continue

        # Handle unordered list items.
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

        # Handle blockquotes.
        if line.startswith("> "):
            blockquote_line = line[2:].strip()
            html_lines.append(f"<blockquote>{blockquote_line}</blockquote>")
            continue

        # Inline formatting: bold, italics, inline code, and links.
        line = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
        line = re.sub(r"\*(.+?)\*", r"<em>\1</em>", line)
        line = re.sub(r"`(.+?)`", r"<code>\1</code>", line)
        line = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', line)

        if line.strip():
            html_lines.append(f"<p>{line}</p>")
        else:
            html_lines.append("")

    # Flush any remaining list items.
    if in_list:
        html_lines.append("<ul>" + "".join(list_buffer) + "</ul>")

    converted = "\n".join(html_lines)
    # CSS styles for Markdown and code highlighting.
    style = """
    <style>
        /* Material-inspired fonts (uses system sans if Roboto is not loaded) */
        body {
            font-family: "Roboto", sans-serif;
            background-color: #fafafa;
            margin: 2rem;
            color: #212121;
        }
        h1, h2, h3, h4, h5, h6 {
            color: #3f51b5; /* Material Indigo */
            margin-bottom: 0.5rem;
        }
        p, li, blockquote {
            line-height: 1.6;
        }
        pre code {
            background-color: #ffffff;
            padding: 1em;
            display: block;
            overflow-x: auto;
            font-family: "Roboto Mono", monospace;
            border-radius: 4px;
            border: 1px solid #e0e0e0;
            margin: 1em 0;
        }
        a {
            color: #1e88e5; /* Material Blue */
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        blockquote {
            border-left: 4px solid #3f51b5;
            background-color: #e8eaf6;
            margin: 1em 0;
            padding: 0.5em 1em;
            border-radius: 4px;
            color: #3f51b5;
        }
        /* Syntax highlighting classes */
        span.keyword { color: #d81b60; font-weight: 500; } /* Pink 600 */
        span.string  { color: #388e3c; }                   /* Green 700 */
        span.number  { color: #f57c00; }                   /* Orange 600 */
        span.comment { color: #757575; font-style: italic;} /* Grey 600 */
    </style>
    """
    return (
        "<!DOCTYPE html>"
        "<html lang='en'>"
        "<head><meta charset='utf-8'><title>Markdown Render</title>"
        + style
        + "</head><body>"
        + converted
        + "</body></html>"
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
            size = item.stat().st_size
            if size < KB:
                formatted_size = f"{size} B"
            elif size < KB**2:
                formatted_size = f"{size / KB:.2f} KB"
            else:
                formatted_size = f"{size / KB**2:.2f} MB"
            items.append(
                f'<li><a href="/{rel_path}">{item.name}</a> ({formatted_size})</li>',
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
