"""Local proxy forwarder that handles upstream proxy authentication.

Chrome doesn't reliably handle proxy auth in headless/automated mode.
This runs a local HTTP proxy (no auth) that forwards CONNECT requests
to an upstream proxy with Basic auth, transparently.

Usage:
    python -m scraper.proxy_forwarder http://user:pass@host:port 8080

Chrome then uses --proxy-server=http://127.0.0.1:8080 (no auth needed).
"""

import asyncio
import base64
import logging
import signal
import sys
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Copy data from reader to writer until EOF."""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


class ProxyForwarder:
    """Local no-auth proxy that forwards to an upstream auth proxy."""

    def __init__(self, upstream_url: str, listen_port: int = 8080):
        parsed = urlparse(upstream_url)
        self.upstream_host = parsed.hostname
        self.upstream_port = parsed.port
        self.listen_port = listen_port
        # Pre-encode auth header
        if parsed.username:
            creds = f"{parsed.username}:{parsed.password or ''}"
            self._auth_header = (
                f"Proxy-Authorization: Basic "
                f"{base64.b64encode(creds.encode()).decode()}\r\n"
            )
        else:
            self._auth_header = ""
        self._server = None

    async def _handle_client(
        self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter
    ):
        try:
            # Read the request line (e.g. "CONNECT host:443 HTTP/1.1\r\n")
            request_line = await asyncio.wait_for(client_r.readline(), timeout=10.0)
            if not request_line:
                client_w.close()
                return

            # Read remaining headers from Chrome
            headers = []
            while True:
                line = await asyncio.wait_for(client_r.readline(), timeout=10.0)
                if line == b"\r\n" or not line:
                    break
                headers.append(line)

            # Connect to upstream proxy
            up_r, up_w = await asyncio.wait_for(
                asyncio.open_connection(self.upstream_host, self.upstream_port),
                timeout=10.0,
            )

            # Forward request with auth header injected
            up_w.write(request_line)
            for h in headers:
                # Skip any existing proxy-auth from Chrome
                if not h.lower().startswith(b"proxy-authorization:"):
                    up_w.write(h)
            up_w.write(self._auth_header.encode())
            up_w.write(b"\r\n")
            await up_w.drain()

            method = request_line.split(b" ")[0].upper()

            if method == b"CONNECT":
                # Read upstream proxy response
                resp_line = await asyncio.wait_for(up_r.readline(), timeout=10.0)
                # Consume upstream response headers
                while True:
                    h = await asyncio.wait_for(up_r.readline(), timeout=10.0)
                    if h == b"\r\n" or not h:
                        break

                if b"200" in resp_line:
                    # Tell Chrome the tunnel is established
                    client_w.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await client_w.drain()
                    # Bidirectional tunnel
                    await asyncio.gather(
                        _pipe(client_r, up_w),
                        _pipe(up_r, client_w),
                    )
                else:
                    # Forward upstream error to Chrome
                    client_w.write(resp_line)
                    client_w.write(b"\r\n")
                    await client_w.drain()
                    client_w.close()
                    up_w.close()
            else:
                # Plain HTTP: forward response back to Chrome
                await asyncio.gather(
                    _pipe(client_r, up_w),
                    _pipe(up_r, client_w),
                )
        except Exception:
            pass
        finally:
            try:
                client_w.close()
            except Exception:
                pass

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle_client, "127.0.0.1", self.listen_port
        )
        logger.info(
            "Proxy forwarder listening on 127.0.0.1:%d -> %s:%s",
            self.listen_port, self.upstream_host, self.upstream_port,
        )

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()


async def run_forwarder(upstream_url: str, port: int):
    fwd = ProxyForwarder(upstream_url, port)
    await fwd.start()
    # Run forever
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await fwd.stop()


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <upstream_proxy_url> [port]")
        sys.exit(1)
    upstream = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_forwarder(upstream, port))


if __name__ == "__main__":
    main()
