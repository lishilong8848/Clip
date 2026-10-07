"""Offline tests for the opt-in audit; never touch source skills or the model."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from .tools import audit_lighthouse_workbuddy_skills as audit
from .tools import import_lighthouse_workbuddy_skills as importer


class SkillAuditTests(unittest.TestCase):
    def test_explicit_user_exclusions(self):
        self.assertEqual(audit.EXCLUDED, {"zhinav-point-data", "告警描述核查"})
        self.assertEqual(audit.EXCLUDED, importer.USER_EXCLUDED_ROOTS)

    def test_walk_never_opens_user_excluded_or_cache_children(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for name in ("zhinav-point-data", "告警描述核查", "__pycache__", "references"):
                folder = root / name
                folder.mkdir()
                (folder / 'data.txt').write_text('fixture', encoding='utf-8')
            self.assertEqual([p.parent.name for p in audit.files(root)], ['references'])

    def test_metadata_distinguishes_syntax_error_from_success(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'SKILL.md').write_text('---\nname: fixture\ndescription: test\n---\n', encoding='utf-8')
            (root / 'bad.py').write_text('def broken(', encoding='utf-8')
            result = audit.metadata(root)
            self.assertEqual(result['errors'], ['python_syntax_errors'])
            self.assertEqual(result['syntax_error_files'], ['bad.py'])

    def test_yaml_error_never_embeds_raw_source(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'SKILL.md').write_text('---\nname: fixture\ndescription: [\n---\n', encoding='utf-8')
            result = audit.metadata(root)
            self.assertEqual(result['errors'], ['invalid_yaml_frontmatter'])
            self.assertNotIn('description: [', json.dumps(result))

    def test_raw_subprocess_output_not_reported(self):
        result = subprocess.CompletedProcess(['test'], 1, 'synthetic-secret', "No module named 'absent_lib': synthetic-secret")
        with self.assertRaisesRegex(RuntimeError, '^missing_dependency:absent_lib$'):
            audit.require_success(result)

    def test_dependency_block_is_not_functional_success(self):
        with patch.object(audit.shutil, 'which', return_value=None):
            status, detail = audit.functional('agent-browser-core', Path('unused'), Path('unused'))
        self.assertEqual(status, 'blocked')
        self.assertIn('agent-browser', detail)

    def test_dry_run_and_apply_cannot_be_combined(self):
        with self.assertRaises(SystemExit) as exc:
            importer.main(['--dry-run', '--apply'])
        self.assertEqual(exc.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
