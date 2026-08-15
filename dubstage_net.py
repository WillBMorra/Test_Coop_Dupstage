# -*- coding: utf-8 -*-
"""Small TCP lobby/sync layer for DubStage. No third-party dependencies."""
import socket, threading, json, base64, uuid

class Peer:
    def __init__(self, sock, addr, on_message, on_close):
        self.sock=sock; self.addr=addr; self.on_message=on_message; self.on_close=on_close
        self.alive=True; self.lock=threading.Lock()
        threading.Thread(target=self._read, daemon=True).start()
    def send(self, obj):
        if not self.alive: return
        data=(json.dumps(obj,separators=(",",":"))+"\n").encode("utf-8")
        try:
            with self.lock: self.sock.sendall(data)
        except Exception: self.close()
    def _read(self):
        buf=b""
        try:
            while self.alive:
                chunk=self.sock.recv(65536)
                if not chunk: break
                buf += chunk
                while b"\n" in buf:
                    raw,buf=buf.split(b"\n",1)
                    if raw:
                        self.on_message(self, json.loads(raw.decode("utf-8")))
        except Exception:
            pass
        self.close()
    def close(self):
        if not self.alive: return
        self.alive=False
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except Exception: pass
        try: self.sock.close()
        except Exception: pass
        try: self.on_close(self)
        except Exception: pass

class Server:
    def __init__(self, on_message, on_connect, on_close):
        self.on_message=on_message; self.on_connect=on_connect; self.on_close=on_close
        self.sock=None; self.clients={}; self.running=False
    def start(self, host="0.0.0.0", port=8765):
        self.sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.sock.bind((host,int(port))); self.sock.listen(16); self.running=True
        threading.Thread(target=self._accept,daemon=True).start()
    def _accept(self):
        while self.running:
            try:
                s,a=self.sock.accept()
                p=Peer(s,a,self.on_message,self._closed)
                self.clients[id(p)]=p
                self.on_connect(p)
            except Exception:
                if self.running: continue
                break
    def _closed(self,p):
        self.clients.pop(id(p),None); self.on_close(p)
    def broadcast(self,obj,exclude=None):
        for p in list(self.clients.values()):
            if p is not exclude: p.send(obj)
    def stop(self):
        self.running=False
        try:self.sock.close()
        except Exception:pass
        for p in list(self.clients.values()): p.close()
        self.clients.clear()

class Client:
    def __init__(self,on_message,on_close):
        self.on_message=on_message; self.on_close=on_close; self.peer=None
    def connect(self,host,port=8765):
        s=socket.create_connection((host,int(port)),timeout=5)
        self.peer=Peer(s,(host,port),lambda p,m:self.on_message(m),lambda p:self.on_close())
    def send(self,obj):
        if self.peer: self.peer.send(obj)
    def close(self):
        if self.peer:self.peer.close()
        self.peer=None

def encode_audio(arr):
    import numpy as np
    a=np.asarray(arr,dtype=np.float32)
    return base64.b64encode(a.tobytes()).decode("ascii")

def decode_audio(s):
    import numpy as np
    return np.frombuffer(base64.b64decode(s.encode("ascii")),dtype=np.float32).copy()
