# 🌐 Singlefile Web Server

[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Stdlib only](https://img.shields.io/badge/python-stdlib--only-2ea44f)](#-features)
[![Single file](https://img.shields.io/badge/architecture-single--file-6f42c1)](#-singlefile-web-server)
[![Asyncio](https://img.shields.io/badge/runtime-asyncio-222222?logo=python&logoColor=white)](#-features)
[![License GPLv3](https://img.shields.io/badge/license-GPLv3-blue.svg)](LICENSE)

This is a simple web server that serves with a single file ⚡️

## ✨ Features

- 🚀 Has no dependencies
- 📂 Renders HTML for folders/directories
- 📝 Renders Markdown
- ⚡️ Asynchronous
- 🌚 Dark and 🌞 light themes

## 🛠️ How to Run

- 🌍 From Web (always latest version)

    ```shell
    curl -LsSf https://raw.githubusercontent.com/benzlokzik/singlefile-webserver/refs/heads/main/server.py | python3
    ```

- 💻 From Code

    ```shell
    python3 server.py
    ```

## ✅ Compatibility

| Platform | `ctypes` | `find_library("c")` | `getifaddrs` | Result |
| --- | --- | --- | --- | --- |
| 🍎 macOS | stdlib `3.9+` | `/usr/lib/libc.dylib` ✅ | BSD API ✅ | ✅ |
| 🐧 Linux (glibc) | stdlib `3.9+` | `libc.so.6` ✅ | glibc `2.3+` ✅ | ✅ |
| 🤖 Android / Termux | stdlib `3.9+` | `libc.so` ✅ | Bionic API `>= 24` | ✅ on modern devices |
| 🐡 FreeBSD / OpenBSD | stdlib `3.9+` | libc via system lookup ✅ | BSD API ✅ | ✅ |
| 🪟 Windows | stdlib `3.9+` | no POSIX libc path | `getifaddrs` unavailable ❌ | ↪ UDP fallback |
| 🕰️ Android `< 7.0` | stdlib `3.9+` | `libc.so` ✅ | unavailable / too old ❌ | ↪ UDP fallback |
| 🚫 Any platform | `3.8` and older | n/a | n/a | ❌ unsupported by current code |

Notes:

- `_network_ips()` uses `getifaddrs()` via `ctypes` first, then falls back to `_ips_via_udp_probe()`.
- Python `3.9+` is required by the current implementation because it uses `pathlib.Path.is_relative_to()`.
- This table describes the LAN IP discovery path used for startup URL printing, not basic file serving alone.

## 📋 TODO

- 🛠️ Build binaries
- 🧠 Optimize render, add more markup languages
- 📥 Add download availability via compressed/archive files (e.g., `.zip`, `.tar.gz`)
