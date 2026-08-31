"""Market Pulse Bot — low-level RFC 6455 WebSocket client (stdlib only).

Pure networking primitives extracted from price_engine.py's inline socket
code — no business logic, no price cache, no exchange-specific parsing.
Shared by price_engine.py and derivatives_engine.py so the raw handshake/
frame-parsing code exists in exactly one place.
"""

import os
import ssl
import socket
import base64
import struct


# ── Low-level RFC 6455 WebSocket client (stdlib only) ────────────────────

def _ws_handshake(host, path, port=443):
    """Open a TLS socket and perform the WebSocket HTTP upgrade handshake.
    Returns the connected ssl.SSLSocket or raises on failure."""
    ctx = ssl.create_default_context()
    raw = socket.create_connection((host, port), timeout=15)
    sock = ctx.wrap_socket(raw, server_hostname=host)
    key = base64.b64encode(os.urandom(16)).decode()
    handshake = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(handshake.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = sock.recv(1024)
        if not chunk:
            raise ConnectionError("WebSocket handshake: empty response")
        resp += chunk
    if b"101" not in resp:
        raise ConnectionError(f"WebSocket upgrade failed: {resp[:200]}")
    return sock


def _ws_recv_frame(sock):
    """Read one complete WebSocket frame. Returns (opcode, payload_bytes).
    Automatically responds to ping frames with a pong (RFC 6455 §5.5.3).
    Raises on connection close or error frames."""
    header = b""
    while len(header) < 2:
        chunk = sock.recv(2 - len(header))
        if not chunk:
            raise ConnectionError("WebSocket: connection closed")
        header += chunk

    byte1, byte2 = header[0], header[1]
    opcode = byte1 & 0x0F
    masked = bool(byte2 & 0x80)
    length = byte2 & 0x7F

    if length == 126:
        raw = _ws_recv_exact(sock, 2)
        length = struct.unpack("!H", raw)[0]
    elif length == 127:
        raw = _ws_recv_exact(sock, 8)
        length = struct.unpack("!Q", raw)[0]

    mask_key = _ws_recv_exact(sock, 4) if masked else b""
    payload = _ws_recv_exact(sock, length)

    if masked:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

    if opcode == 8:   # Connection close
        raise ConnectionError("WebSocket: server sent close frame")
    if opcode == 9:   # Ping — must reply with Pong (opcode 0xA) or server disconnects
        _ws_send_pong(sock, payload)
        return None, b""
    if opcode == 0xA: # Pong — unsolicited pong, just ignore
        return None, b""
    return opcode, payload


def _ws_send_pong(sock, payload=b""):
    """Send a RFC 6455 Pong frame (opcode 0xA), client-masked."""
    length = len(payload)
    mask_key = os.urandom(4)
    masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    if length <= 125:
        header = bytes([0x8A, 0x80 | length]) + mask_key
    else:
        header = bytes([0x8A, 0x7E]) + struct.pack("!H", length) + mask_key
    sock.sendall(header + masked)


def _ws_recv_exact(sock, n):
    """Read exactly n bytes from sock."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("WebSocket: short read")
        buf += chunk
    return buf


def _ws_send_text(sock, text):
    """Send a text frame (opcode 1), client-masked as per RFC 6455."""
    payload = text.encode("utf-8")
    length = len(payload)
    mask_key = os.urandom(4)
    masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    if length <= 125:
        header = bytes([0x81, 0x80 | length]) + mask_key
    elif length <= 65535:
        header = bytes([0x81, 0xFE]) + struct.pack("!H", length) + mask_key
    else:
        header = bytes([0x81, 0xFF]) + struct.pack("!Q", length) + mask_key
    sock.sendall(header + masked_payload)


