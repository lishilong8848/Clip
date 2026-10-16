"""Isolated tests (unittest only) for shared knowledge base nested folders.

Covers the additive ``parent_id`` migration, create parent/child folders, rename
with re-parent (cycle rejection), missing-parent rejection, permission
enforcement, batch document moves into/out of folders, and folder deletion that
reparents child folders and ungroups files without ever deleting files/index.

Everything runs against a temporary SQLite DB with a deterministic 512-dim
embedder and a real FAISS in-memory index, worker disabled, no production data
or network.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor, _protect, _text


class KnowledgeFoldersTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = self._tmp.name
        self.kb = KnowledgeBase(self.root, embedder=FakeEmbedder(), protect=_protect,
                                start_worker=False, memory_index_factory=FakeMemoryIndex)

    def _reopen(self):
        return KnowledgeBase(self.root, embedder=FakeEmbedder(), protect=_protect,
                             start_worker=False, memory_index_factory=FakeMemoryIndex)

    def _upload(self, owner, content, filename):
        doc = self.kb.upload(owner, *_text(content, filename))
        self.assertTrue(self.kb.process_one())
        return doc

    def _parents(self):
        with self.kb.connect() as db:
            return {row['id']: row['parent_id'] for row in db.execute(
                'SELECT id,parent_id FROM knowledge_groups')}

    def test_additive_parent_id_migration(self):
        """Existing flat-group DB gains parent_id without data loss or new tables."""
        admin = actor("admin", admin=True)
        root1 = self.kb.create_group(admin, "根目录")
        root2 = self.kb.create_group(admin, "另一个根")
        self.assertIsNone(root1["parent_id"])
        self.assertIsNone(root2["parent_id"])

        child = self.kb.create_group(admin, "子目录", parent_id=root1["id"])
        self.assertEqual(child["parent_id"], root1["id"])
        grand = self.kb.create_group(admin, "孙目录", parent_id=child["id"])
        self.assertEqual(grand["parent_id"], child["id"])

        # Reopen on the same file: schema migration is idempotent and flat data intact.
        kb2 = self._reopen()
        try:
            groups = kb2.groups(admin)["groups"]
            by_id = {g["id"]: g for g in groups}
            self.assertEqual(by_id[root1["id"]]["name"], "根目录")
            self.assertEqual(by_id[root1["id"]]["parent_id"], None)
            self.assertEqual(by_id[child["id"]]["parent_id"], root1["id"])
            self.assertEqual(len(groups), 4)
        finally:
            kb2.close()

    def test_create_rejects_missing_parent(self):
        owner = actor()
        with self.assertRaises(AssistantError) as ctx:
            self.kb.create_group(owner, "孤儿", parent_id="no-such-folder")
        self.assertEqual(ctx.exception.status, 404)

    def test_rename_move_cycle_rejected(self):
        admin = actor("admin", admin=True)
        a = self.kb.create_group(admin, "A")
        b = self.kb.create_group(admin, "B", parent_id=a["id"])
        c = self.kb.create_group(admin, "C", parent_id=b["id"])

        # Move B under C would form a cycle (C is B's descendant).
        with self.assertRaises(AssistantError) as ctx:
            self.kb.rename_group(admin, b["id"], "B", parent_id=c["id"])
        self.assertEqual(ctx.exception.status, 409)

        # Moving a folder under itself is rejected too.
        with self.assertRaises(AssistantError) as ctx:
            self.kb.rename_group(admin, b["id"], "B", parent_id=b["id"])
        self.assertEqual(ctx.exception.status, 409)

        # A legal move still works and keeps descendants' parent chain consistent.
        renamed = self.kb.rename_group(admin, b["id"], "BB", parent_id=a["id"])
        self.assertEqual(renamed["parent_id"], a["id"])
        self.assertEqual(renamed["name"], "BB")

        # Missing new parent rejected.
        with self.assertRaises(AssistantError) as ctx:
            self.kb.rename_group(admin, c["id"], "C", parent_id="missing")
        self.assertEqual(ctx.exception.status, 404)

    def test_rename_without_parent_preserves_nested_location(self):
        owner = actor()
        root = self.kb.create_group(owner, '根')
        child = self.kb.create_group(owner, '子', parent_id=root['id'])
        renamed = self.kb.rename_group(owner, child['id'], '改名')
        self.assertEqual(renamed['parent_id'], root['id'])
        moved = self.kb.rename_group(owner, child['id'], '改名', parent_id=None)
        self.assertIsNone(moved['parent_id'])

    def test_rename_move_permission_checked(self):
        alice = actor("alice")
        bob = actor("bob")
        a = self.kb.create_group(alice, "A")
        b = self.kb.create_group(bob, "B")
        # bob cannot move alice's folder.
        with self.assertRaises(AssistantError) as ctx:
            self.kb.rename_group(bob, a["id"], "A2", parent_id=b["id"])
        self.assertEqual(ctx.exception.status, 403)
        # creating a child under a foreign parent is allowed (any signed-in user),
        # but cannot rename/delete that parent itself.
        child = self.kb.create_group(bob, "bob-child", parent_id=a["id"])
        self.assertEqual(child["parent_id"], a["id"])
        with self.assertRaises(AssistantError) as ctx:
            self.kb.delete_group(bob, a["id"])
        self.assertEqual(ctx.exception.status, 403)

    def test_delete_reparents_children_and_ungroups_files(self):
        admin = actor("admin", admin=True)
        parent = self.kb.create_group(admin, "父")
        child1 = self.kb.create_group(admin, "子1", parent_id=parent["id"])
        child2 = self.kb.create_group(admin, "子2", parent_id=parent["id"])
        grand = self.kb.create_group(admin, "孙", parent_id=child1["id"])

        parent_doc = self._upload(admin, "父目录文件说明。", "parent.txt")
        child_doc = self._upload(admin, "子目录文件说明。", "child.txt")
        grand_doc = self._upload(admin, "孙目录文件说明。", "grand.txt")
        self.kb.set_documents_group(admin, parent["id"], [parent_doc["id"]])
        self.kb.set_documents_group(admin, child1["id"], [child_doc["id"]])
        self.kb.set_documents_group(admin, grand["id"], [grand_doc["id"]])

        # Deleting the parent must NOT delete children or files silently.
        self.assertTrue(self.kb.delete_group(admin, parent["id"])["deleted"])
        parents = self._parents()
        # children are reparented to the deleted parent's parent (root).
        self.assertIsNone(parents[child1["id"]])
        self.assertIsNone(parents[child2["id"]])
        # grandchild follows its own chain (still under child1).
        self.assertEqual(parents[grand["id"]], child1["id"])

        # Documents that were in the deleted folder return to ungrouped.
        ungrouped = self.kb.list(admin, {"page": 1, "group": "ungrouped"})
        ids = {item["id"] for item in ungrouped["items"]}
        self.assertIn(parent_doc["id"], ids)
        self.assertNotIn(child_doc["id"], ids)
        self.assertNotIn(grand_doc["id"], ids)

        # Files/index untouched.
        with self.kb.connect() as db:
            chunks = db.execute('SELECT count(*) FROM chunks WHERE document_id=?', (parent_doc["id"],)).fetchone()[0]
            vectors = db.execute('SELECT count(*) FROM local_vectors lv JOIN chunks c ON c.id=lv.chunk_id '
                                 'WHERE c.document_id=?', (parent_doc["id"],)).fetchone()[0]
        self.assertGreater(chunks, 0)
        self.assertEqual(vectors, chunks)

    def test_descendant_reparent_chain_preserved_on_deep_delete(self):
        """Deleting a mid-level folder reparents its direct children to its parent."""
        admin = actor("admin", admin=True)
        top = self.kb.create_group(admin, "顶层")
        mid = self.kb.create_group(admin, "中层", parent_id=top["id"])
        low = self.kb.create_group(admin, "底层", parent_id=mid["id"])
        deep = self.kb.create_group(admin, "更深", parent_id=low["id"])

        self.assertTrue(self.kb.delete_group(admin, low["id"])["deleted"])
        parents = self._parents()
        self.assertEqual(parents[deep["id"]], mid["id"])
        self.assertEqual(parents[mid["id"]], top["id"])

    def test_batch_move_between_folders_and_ungrouped(self):
        admin = actor("admin", admin=True)
        root = self.kb.create_group(admin, "根")
        sub = self.kb.create_group(admin, "子", parent_id=root["id"])
        docs = [self._upload(admin, f"内容{idx}说明。", f"f{idx}.txt") for idx in range(4)]

        self.kb.set_documents_group(admin, root["id"], [docs[0]["id"], docs[1]["id"]])
        self.kb.set_documents_group(admin, sub["id"], [docs[2]["id"]])

        # Move the two root docs down into the child folder.
        result = self.kb.set_documents_group(admin, sub["id"], [docs[0]["id"], docs[1]["id"]])
        self.assertEqual(result["updated"], 2)
        self.assertEqual(self.kb.list(admin, {"page": 1, "group": sub["id"]})["total"], 3)
        self.assertEqual(self.kb.list(admin, {"page": 1, "group": root["id"]})["total"], 0)

        # Batch move back to ungrouped (docs[0], docs[2] were ungrouped before;
        # docs[3] was never grouped, so ungrouped now contains docs[0], docs[2], docs[3]).
        self.kb.set_documents_group(admin, None, [docs[0]["id"], docs[2]["id"]])
        self.assertEqual(self.kb.list(admin, {"page": 1, "group": "ungrouped"})["total"], 3)

        # Counts reflect direct membership per folder.
        groups = {g["id"]: g["count"] for g in self.kb.groups(admin)["groups"]}
        self.assertEqual(groups[sub["id"]], 1)  # only docs[1] remains in sub
        self.assertEqual(groups[root["id"]], 0)

    def test_move_permissions_atomic_reject(self):
        alice = actor("alice")
        bob = actor("bob")
        folder = self.kb.create_group(alice, "共享目录")
        alice_doc = self._upload(alice, "alice内容描述。", "alice.txt")
        bob_doc = self._upload(bob, "bob内容描述。", "bob.txt")
        # Alice cannot move bob's file into her folder; nothing changes atomically.
        with self.assertRaises(AssistantError) as ctx:
            self.kb.set_documents_group(alice, folder["id"], [alice_doc["id"], bob_doc["id"]])
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(self.kb.groups(alice)["groups"][0]["count"], 0)

    def test_existing_flat_groups_remain_roots_and_groups_api_compatible(self):
        owner = actor("owner")
        g = self.kb.create_group(owner, "平铺组")
        listing = self.kb.groups(owner)["groups"]
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0]["id"], g["id"])
        self.assertIsNone(listing[0]["parent_id"])
        # The group has the expected count (0) and can_manage flag.
        self.assertEqual(listing[0]["count"], 0)
        self.assertTrue(listing[0]["can_manage"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
