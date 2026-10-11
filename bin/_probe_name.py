import sys, tempfile, hashlib, math
from pathlib import Path
sys.path.insert(0, str(Path(r"D:/桌面/ShiJian_Code/pythonProject/上传维保变更设备调整/bin").resolve()))
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase, DIMENSIONS

def actor(uid="u1", *, admin=False, guest=False, role=None, name="user"):
    role = role or ("guest" if guest else ("admin" if admin else "user"))
    return {"id": uid, "is_admin": admin, "is_guest": guest, "role": role, "name": name}

class FakeEmbedder:
    def __init__(self, dim=DIMENSIONS):
        self.dim=dim
    def vector_for(self, text):
        digest=hashlib.sha256(text.encode()).digest()
        vec=[(digest[i%len(digest)]/255.0)*2-1 for i in range(self.dim)]
        norm=math.sqrt(sum(v*v for v in vec)) or 1.0
        return [v/norm for v in vec]
    def embed(self, texts, config=None, *, query=False):
        return [self.vector_for(t) for t in texts]
    def close(self):
        pass

with tempfile.TemporaryDirectory() as d:
    kb = KnowledgeBase(d, embedder=FakeEmbedder(), start_worker=False)
    name = "folder" + chr(92) + "file.txt"  # unambiguous single backslash
    print("name passed:", repr(name))
    # manual trace of KB sanitize
    step = str(name or "").replace("\\\\", "/").split("/")[-1].strip()
    print("manual basename:", repr(step))
    res = kb.upload(actor(admin=True), name, b"gongsi baoxiao liucheng")
    print("result name:", repr(res["name"]))
    with kb.connect() as db:
        row = db.execute("SELECT name FROM documents WHERE id=?", (res["id"],)).fetchone()
        print("db name:", repr(row[0]))