# -*- coding: utf-8 -*-
"""Small TCP JSON-lines networking layer for DubStage co-op."""
import socket, threading, json, base64, queue, time

class CoopPeer:
    def __init__(self, sock, addr, name=""):
        self.sock=sock; self.addr=addr; self.name=name
        self.send_lock=threading.Lock(); self.alive=True
    def send(self, obj):
        if not self.alive: return False
        try:
            raw=(json.dumps(obj,separators=(",",":"),ensure_ascii=False)+"\n").encode("utf-8")
            with self.send_lock: self.sock.sendall(raw)
            return True
        except Exception:
            self.alive=False; return False
    def close(self):
        self.alive=False
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except Exception: pass
        try: self.sock.close()
        except Exception: pass

class CoopHost:
    def __init__(self, bind="0.0.0.0", port=8765):
        self.bind=bind; self.port=int(port); self.server=None
        self.peers=[]; self.events=queue.Queue(); self.running=False
    def start(self):
        self.server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.server.bind((self.bind,self.port)); self.server.listen(8)
        self.running=True
        threading.Thread(target=self._accept,daemon=True).start()
    def _accept(self):
        while self.running:
            try: s,a=self.server.accept()
            except Exception: break
            p=CoopPeer(s,a); self.peers.append(p)
            threading.Thread(target=self._reader,args=(p,),daemon=True).start()
    def _reader(self,p):
        buf=b""
        try:
            while self.running and p.alive:
                data=p.sock.recv(65536)
                if not data: break
                buf+=data
                while b"\n" in buf:
                    raw,buf=buf.split(b"\n",1)
                    if not raw: continue
                    try: msg=json.loads(raw.decode("utf-8"))
                    except Exception: continue
                    self.events.put((p,msg))
        finally:
            p.close()
            try: self.peers.remove(p)
            except ValueError: pass
            self.events.put((p,{"type":"disconnect"}))
    def broadcast(self,msg):
        for p in list(self.peers):
            if not p.send(msg):
                try:self.peers.remove(p)
                except ValueError:pass
    def send(self,p,msg): p.send(msg)
    def stop(self):
        self.running=False
        for p in list(self.peers): p.close()
        self.peers=[]
        try:self.server.close()
        except Exception:pass

class CoopClient:
    def __init__(self, host, port=8765):
        self.host=host; self.port=int(port); self.sock=None
        self.events=queue.Queue(); self.running=False
        self.send_lock=threading.Lock()
    def start(self,name):
        self.sock=socket.create_connection((self.host,self.port),timeout=5)
        self.sock.settimeout(None); self.running=True
        self.send({"type":"hello","name":name})
        threading.Thread(target=self._reader,daemon=True).start()
    def send(self,obj):
        if not self.running:return False
        try:
            raw=(json.dumps(obj,separators=(",",":"),ensure_ascii=False)+"\n").encode("utf-8")
            with self.send_lock:self.sock.sendall(raw)
            return True
        except Exception:
            self.running=False; return False
    def _reader(self):
        buf=b""
        try:
            while self.running:
                data=self.sock.recv(65536)
                if not data: break
                buf+=data
                while b"\n" in buf:
                    raw,buf=buf.split(b"\n",1)
                    if raw:
                        try:self.events.put(json.loads(raw.decode("utf-8")))
                        except Exception:pass
        finally:
            self.running=False; self.events.put({"type":"disconnect"})
    def stop(self):
        self.running=False
        try:self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:pass
        try:self.sock.close()
        except Exception:pass

def pack_audio(data):
    return base64.b64encode(bytes(data)).decode("ascii")
def unpack_audio(s):
    return base64.b64decode(s.encode("ascii"))
