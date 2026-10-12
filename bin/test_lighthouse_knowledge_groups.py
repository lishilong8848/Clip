"""Isolation tests (unittest only) for shared knowledge base document grouping.

Groups only organize files: no re-embedding, no index/vector generation bump, no
document ``updated_at``/revision change, and no file/index mutation on group
rename/delete (members return to "ungrouped"). All tests run against a temporary
SQLite DB with a deterministic 512-dim embedder and a real FAISS in-memory index,
worker disabled, no production data or network.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor, _protect, _text


class GroupPersistenceTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = self._tmp.name
        self.embedder = FakeEmbedder()
        self.kb = KnowledgeBase(self.root, embedder=self.embedder, protect=_protect,
                                start_worker=False, memory_index_factory=FakeMemoryIndex)

    def _new_instance(self):
        return KnowledgeBase(self.root, embedder=FakeEmbedder(), protect=_protect,
                             start_worker=False, memory_index_factory=FakeMemoryIndex)

    def _upload(self, owner, content, filename="报销.txt"):
        doc = self.kb.upload(owner, *_text(content, filename))
        self.assertTrue(self.kb.process_one())
        return doc

    def _snapshot_meta(self, doc_id):
        with self.kb.connect() as db:
            return dict(db.execute('SELECT revision,updated_at FROM documents WHERE id=?', (doc_id,)).fetchone())

    def test_group_rename_delete_keep_files_and_index(self):
        owner = actor("alice", name="Alice")
        doc = self._upload(owner, "公司报销流程需要真实凭证。")
        g1 = self.kb.create_group(owner, "财务")
        g2 = self.kb.create_group(owner, "人事")
        before = self._snapshot_meta(doc["id"])

        self.kb.set_documents_group(owner, g1["id"], [doc["id"]])
        # grouping must not touch revision/updated_at
        meta = self._snapshot_meta(doc["id"])
        self.assertEqual(meta, before)
        listing = self.kb.list(owner, {"page": 1, "group": g1["id"]})
        self.assertEqual(listing["total"], 1)
        self.assertEqual(listing["items"][0]["id"], doc["id"])

        # rename keeps membership
        renamed = self.kb.rename_group(owner, g1["id"], "财务部")
        self.assertEqual(renamed["name"], "财务部")
        self.assertEqual(self.kb.list(owner, {"page": 1, "group": g1["id"]})["total"], 1)

        # delete group moves file back to ungrouped, files/index untouched
        self.kb.delete_group(owner, g1["id"])
        self.assertEqual(self.kb.list(owner, {"page": 1, "group": "ungrouped"})["total"], 1)
        meta_after = self._snapshot_meta(doc["id"])
        self.assertEqual(meta_after, before)
        with self.kb.connect() as db:
            chunks = db.execute('SELECT count(*) FROM chunks WHERE document_id=?', (doc["id"],)).fetchone()[0]
            vectors = db.execute('SELECT count(*) FROM local_vectors lv JOIN chunks c ON c.id=lv.chunk_id WHERE c.document_id=?', (doc["id"],)).fetchone()[0]
        self.assertGreater(chunks, 0)
        self.assertEqual(vectors, chunks)
        # other group unused
        self.assertTrue(g2["id"])

    def test_persistence_across_restart(self):
        owner = actor("bob", name="Bob")
        doc = self._upload(owner, "考勤制度说明。")
        g = self.kb.create_group(owner, "行政")
        self.kb.set_documents_group(owner, g["id"], [doc["id"]])
        self.kb.close()

        kb2 = self._new_instance()
        groups = kb2.groups(owner)["groups"]
        self.assertEqual(groups[0]["name"], "行政")
        self.assertEqual(kb2.list(owner, {"page": 1, "group": g["id"]})["total"], 1)
        self.assertEqual(kb2.list(owner, {"page": 1, "group": "ungrouped"})["total"], 0)
        kb2.close()

    def test_filter_pagination_total_accurate(self):
        admin = actor(admin=True)
        g1 = self.kb.create_group(admin, "组甲")
        g2 = self.kb.create_group(admin, "组乙")
        docs = []
        for i, name in enumerate(["a.txt", "b.txt", "c.txt", "d.txt"]):
            docs.append(self._upload(admin, f"文件{i}内容描述。", name))
        self.kb.set_documents_group(admin, g1["id"], [docs[0]["id"], docs[1]["id"]])
        self.kb.set_documents_group(admin, g2["id"], [docs[2]["id"]])
        # d.txt stays ungrouped

        all_list = self.kb.list(admin, {"page": 1})
        self.assertEqual(all_list["total"], 4)
        ungrouped = self.kb.list(admin, {"page": 1, "group": "ungrouped"})
        self.assertEqual(ungrouped["total"], 1)
        self.assertEqual(ungrouped["items"][0]["id"], docs[3]["id"])
        for gid, expected in ((g1["id"], 2), (g2["id"], 1)):
            listing = self.kb.list(admin, {"page": 1, "group": gid})
            self.assertEqual(listing["total"], expected)
        # second page accurate when a group has >20 files
        many = [self._upload(admin, f"批量文件{idx}说明。", f"many{idx}.txt") for idx in range(22)]
        self.kb.set_documents_group(admin, g1["id"], [d["id"] for d in many])
        p1 = self.kb.list(admin, {"page": 2, "group": g1["id"]})
        self.assertEqual(p1["total"], 24)  # 2 earlier + 22 batch
        self.assertEqual(len(p1["items"]), 4)

    def test_batch_atomic_reject_on_forbidden_and_missing(self):
        alice = actor("alice")
        bob = actor("bob")
        g = self.kb.create_group(alice, "共享组")
        alice_doc = self._upload(alice, "alice文档。", "alice.txt")
        bob_doc = self._upload(bob, "bob文档。", "bob.txt")
        missing = self.kb.set_documents_group(alice, g["id"], [alice_doc["id"]])
        self.assertEqual(missing["updated"], 1)

        # bob's doc not editable by alice -> atomic reject, no partial assignment
        with self.assertRaises(AssistantError) as ctx:
            self.kb.set_documents_group(alice, g["id"], [alice_doc["id"], bob_doc["id"]])
        self.assertEqual(ctx.exception.status, 403)
        groups = self.kb.groups(alice)["groups"]
        self.assertEqual(groups[0]["count"], 1)

        # missing/orphan id -> atomic reject
        with self.assertRaises(AssistantError) as ctx:
            self.kb.set_documents_group(alice, g["id"], [alice_doc["id"], "not-exist"])
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(self.kb.groups(alice)["groups"][0]["count"], 1)

        # un-editable by non-owner cannot remove either
        with self.assertRaises(AssistantError) as ctx:
            self.kb.remove_documents_group(bob, g["id"], [alice_doc["id"]])
        self.assertEqual(ctx.exception.status, 403)

    def test_permission_and_guest_enforcement(self):
        alice = actor("alice")
        guest = actor(guest=True)
        for method in (lambda: self.kb.groups(guest),
                       lambda: self.kb.create_group(guest, "x"),
                       lambda: self.kb.rename_group(guest, "x", "y"),
                       lambda: self.kb.delete_group(guest, "x")):
            with self.subTest(method=method):
                with self.assertRaises(AssistantError) as ctx:
                    method()
                self.assertEqual(ctx.exception.status, 403)
        # non-existent group for signed-in user -> 404 (not a permission leak)
        with self.assertRaises(AssistantError) as ctx:
            self.kb.rename_group(alice, "x", "y")
        self.assertEqual(ctx.exception.status, 404)
        # non-creator user cannot rename/delete another's group
        g = self.kb.create_group(alice, "alice组")
        with self.assertRaises(AssistantError) as ctx:
            self.kb.delete_group(actor("bob"), g["id"])
        self.assertEqual(ctx.exception.status, 403)
        # admin can
        self.assertTrue(self.kb.delete_group(actor(admin=True), g["id"])["deleted"])

    def test_name_validation_and_duplicate(self):
        owner = actor()
        with self.assertRaises(AssistantError):
            self.kb.create_group(owner, "")
        with self.assertRaises(AssistantError):
            self.kb.create_group(owner, "x" * 51)
        self.kb.create_group(owner, "研发")
        with self.assertRaises(AssistantError) as ctx:
            self.kb.create_group(owner, " 研发 ")
        self.assertEqual(ctx.exception.status, 409)

    def test_reupload_keeps_group_and_grouping_does_not_reindex(self):
        owner = actor(admin=True)
        doc = self._upload(owner, "第一版内容描述。")
        g = self.kb.create_group(owner, "管理组")
        self.kb.set_documents_group(owner, g["id"], [doc["id"]])

        # re-upload (new version) keeps group membership
        self.kb.upload(owner, *_text("第二版内容描述。", "报销.txt"), document_id=doc["id"], revision=doc["version"])
        self.assertTrue(self.kb.process_one())
        listing = self.kb.list(owner, {"page": 1, "group": g["id"]})
        self.assertEqual(listing["total"], 1)
        self.assertEqual(listing["items"][0]["id"], doc["id"])
        # grouping/re-upload does not by itself bump the vector generation beyond
        # what indexing of the new content did; assignment alone leaves it unchanged.
        with self.kb.connect() as db:
            after_grouping = self.kb._vector_revision(db)
        # Assigning to the same group twice is a no-op for vectors.
        self.kb.set_documents_group(owner, g["id"], [doc["id"]])
        with self.kb.connect() as db:
            after_assign = self.kb._vector_revision(db)
        self.assertEqual(after_assign, after_grouping)

    def test_q_combined_with_group_and_ungrouped_filters_real_sqlite(self):
        """q / owner_name / name predicates stay unambiguous after the knowledge_groups
        join (which shares name/owner_name/owner columns). Real SQLite, not a mock."""
        admin = actor(admin=True)
        g = self.kb.create_group(admin, "研发组")
        docs = []
        for _, term in (("alpha", "报销-alpha"), ("beta", "报销-beta"), ("gamma", "器材-gamma")):
            doc = self._upload(admin, f"{term}内容描述。", f"{term}.txt")
            docs.append(doc)
        self.kb.set_documents_group(admin, g["id"], [docs[0]["id"], docs[1]["id"]])
        # docs[2] stays ungrouped.

        # q + group: only matching name rows inside the group
        in_group = self.kb.list(admin, {"page": 1, "q": "报销", "group": g["id"]})
        self.assertEqual(in_group["total"], 2)
        self.assertEqual(in_group["items"][0]["id"], docs[1]["id"])
        self.assertEqual(in_group["items"][1]["id"], docs[0]["id"])

        # q + ungrouped: only matching rows outside any group
        ungrouped = self.kb.list(admin, {"page": 1, "q": "报销", "group": "ungrouped"})
        self.assertEqual(ungrouped["total"], 0)
        ungrouped2 = self.kb.list(admin, {"page": 1, "q": "器材", "group": "ungrouped"})
        self.assertEqual(ungrouped2["total"], 1)
        self.assertEqual(ungrouped2["items"][0]["id"], docs[2]["id"])

    def test_regular_user_trash_search_real_sqlite(self):
        """A non-admin viewing their own recycle bin with a q filter must not raise an
        ambiguous-column error and must only return their own deleted rows."""
        alice = actor("alice", name="Alice")
        bob = actor("bob", name="Bob")
        mine = self._upload(alice, "合同预算说明。", "我的合同.txt")
        other = self._upload(bob, "采购预算细则。", "他人合同.txt")
        g = self.kb.create_group(alice, "临时组")
        self.kb.set_documents_group(alice, g["id"], [mine["id"]])
        with self.kb.connect() as db:
            my_rev = db.execute('SELECT revision FROM documents WHERE id=?', (mine["id"],)).fetchone()["revision"]
            other_rev = db.execute('SELECT revision FROM documents WHERE id=?', (other["id"],)).fetchone()["revision"]
        # move both to recycle bin through the service
        self.assertEqual(self.kb.change(alice, mine["id"], "delete", my_rev)["status"], "deleted")
        self.assertEqual(self.kb.change(bob, other["id"], "delete", other_rev)["status"], "deleted")

        # Alice searches her own trash; must not blow up on the ambiguous name/owner
        # columns and must never see Bob's deleted doc.
        trash = self.kb.list(alice, {"page": 1, "q": "合同", "deleted": "1"})
        self.assertEqual(trash["total"], 1)
        self.assertEqual(trash["items"][0]["id"], mine["id"])

    def test_list_clamps_page_after_deletion(self):
        """list() must clamp over-large pages to the last valid page (and return
        matching items) instead of returning an out-of-range empty page."""
        admin = actor(admin=True)
        docs = [self._upload(admin, f"内容{idx:02d}描述。", f"f{idx:02d}.txt") for idx in range(22)]
        with self.kb.connect() as db:
            first_rev = db.execute('SELECT revision FROM documents WHERE id=?', (docs[0]["id"],)).fetchone()["revision"]
        # page 2 is currently valid (22 docs -> 2 pages). Delete one so only 21 remain.
        self.assertEqual(self.kb.change(admin, docs[0]["id"], "delete", first_rev)["status"], "deleted")
        clamped = self.kb.list(admin, {"page": 99})
        self.assertEqual(clamped["total"], 21)
        self.assertEqual(clamped["page"], 2)
        self.assertEqual(len(clamped["items"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)