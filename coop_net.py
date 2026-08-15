# -*- coding: utf-8 -*-
"""Small TCP co-op layer for DubStage.

Protocol is length-prefixed JSON. Audio takes are sent as base64 WAV data.
This intentionally uses TCP and also supports 127.0.0.1 for same-PC testing.
"""
import base64
import json
import socket
import struct
import threading
import wave
import io


def wav_bytes(samples, sr=44100):
    import numpy as np
    a = np.clip(np.asarray(samples, dtype=np.float32), -1, 1)
    raw = (a * 32767).astype("<i2").tobytes()
    b = io.BytesIO()
    with wave.open(b, "wb") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(sr)
        w.writeframes(raw)
    return b.getvalue()


def wav_samples(data, sr=44100):
    import numpy as np
    with wave.open(io.BytesIO(data), "rb") as w:
        raw = w.readframes(w.getnframes())
        rate = w.getframerate()
    a = np.frombuffer(raw, dtype="<i2").astype(np.float32) / 32768.0
    if rate != sr:
        # DubStage normally uses 44100; avoid an extra dependency here.
        # The protocol still carries the original rate.
        raise ValueError("Unsupported take sample rate: %s" % rate)
    return a


def _send(sock, obj):
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    sock.sendall(struct.pack("!I", len(raw)) + raw)


def _recv(sock):
    head = _readn(sock, 4)
    if not head:
        return None
    n = struct.unpack("!I", head)[0]
    if n <= 0 or n > 64 * 1024 * 1024:
        raise ValueError("Invalid packet size")
    raw = _readn(sock, n)
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8"))


def _readn(sock, n):
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            return None
        out.extend(chunk)
    return bytes(out)


class CoopHost:
    def __init__(self, on_message):
        self.on_message = on_message
        self.sock = None
        self.clients = {}
        self.lock = threading.Lock()
        self.running = False
        self.next_id = 1
        self.port = None
        self.host_name = 'Host'

    def start(self, port=8765, host_name='Host'):
        self.host_name = str(host_name or 'Host')[:32]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 0.0.0.0 is important: localhost works and LAN clients can connect.
        self.sock.bind(("0.0.0.0", int(port)))
        self.sock.listen(16)
        self.sock.settimeout(0.5)
        self.port = self.sock.getsockname()[1]
        self.running = True
        threading.Thread(target=self._accept_loop, daemon=True).start()
        return self.port

    def _accept_loop(self):
        while self.running:
            try:
                c, addr = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            cid = self.next_id; self.next_id += 1
            threading.Thread(target=self._client_loop,
                             args=(cid, c, addr), daemon=True).start()

    def _client_loop(self, cid, sock, addr):
        try:
            hello = _recv(sock)
            if not hello or hello.get("type") != "hello":
                sock.close(); return
            name = str(hello.get("name") or ("Player %d" % cid))[:32]
            with self.lock:
                self.clients[cid] = {"sock": sock, "name": name, "addr": addr}
            self.on_message({"type": "connected", "id": cid, "name": name})
            self.broadcast_players()
            while self.running:
                msg = _recv(sock)
                if msg is None:
                    break
                msg["_client_id"] = cid
                self.on_message(msg)
        except Exception as e:
            self.on_message({"type": "net_error", "error": str(e)})
        finally:
            with self.lock:
                info = self.clients.pop(cid, None)
            try: sock.close()
            except Exception: pass
            if info:
                self.on_message({"type": "disconnected", "id": cid,
                                 "name": info["name"]})
                self.broadcast_players()

    def send(self, cid, msg):
        with self.lock:
            info = self.clients.get(cid)
        if not info:
            return False
        try:
            _send(info["sock"], msg)
            return True
        except Exception:
            return False

    def broadcast(self, msg, include_host=True):
        with self.lock:
            items = list(self.clients.items())
        for cid, info in items:
            try:
                _send(info["sock"], msg)
            except Exception:
                pass

    def broadcast_players(self):
        with self.lock:
            players = [{"id": 0, "name": self.host_name}] + [
                {"id": cid, "name": v["name"]}
                for cid, v in self.clients.items()
            ]
        self.broadcast({"type": "players", "players": players})

    def stop(self):
        self.running = False
        try: self.sock.close()
        except Exception: pass
        with self.lock:
            items = list(self.clients.values())
            self.clients.clear()
        for info in items:
            try: info["sock"].close()
            except Exception: pass


class CoopClient:
    def __init__(self, on_message):
        self.on_message = on_message
        self.sock = None
        self.running = False

    def connect(self, host, port, name, timeout=5):
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.settimeout(None)
        _send(s, {"type": "hello", "name": name})
        self.sock = s
        self.running = True
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        try:
            while self.running:
                msg = _recv(self.sock)
                if msg is None:
                    break
                self.on_message(msg)
        except Exception as e:
            if self.running:
                self.on_message({"type": "net_error", "error": str(e)})
        finally:
            self.running = False
            self.on_message({"type": "closed"})

    def send(self, msg):
        if not self.sock or not self.running:
            return False
        try:
            _send(self.sock, msg)
            return True
        except Exception:
            return False

    def stop(self):
        self.running = False
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except Exception: pass
        try: self.sock.close()
        except Exception: pass
