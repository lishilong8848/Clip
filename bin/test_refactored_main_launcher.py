import ast
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


MAIN_PATH = Path(__file__).resolve().parent / "refactored_main.py"


class RefactoredMainLauncherTests(unittest.TestCase):
    def test_windows_reexec_waits_for_project_python(self):
        tree = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))
        function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "_reexec_with_project_python"
        )
        call = Mock(return_value=7)
        namespace = {
            "__name__": "__main__",
            "__file__": str(MAIN_PATH),
            "_LAUNCH_PARENT_ARG": "--clipflow-launch-parent-pid",
            "os": os,
            "Path": Path,
            "subprocess": SimpleNamespace(call=call),
            "sys": SimpleNamespace(
                argv=[str(MAIN_PATH)],
                executable=os.environ.get("ComSpec", r"C:\Windows\System32\cmd.exe"),
                platform="win32",
            ),
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(MAIN_PATH), "exec"), namespace)

        with self.assertRaisesRegex(SystemExit, "7"):
            namespace["_reexec_with_project_python"]()
        call.assert_called_once()
        argv = call.call_args.args[0]
        self.assertIn("--clipflow-launch-parent-pid", argv)

    def test_patch_restart_waits_until_previous_instance_exits(self):
        tree = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))
        function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "_wait_for_previous_instance"
        )
        checks = iter((True, True, False))
        namespace = {
            "time": SimpleNamespace(monotonic=Mock(side_effect=(0.0, 0.1, 0.2)), sleep=Mock()),
            "is_already_running": Mock(side_effect=lambda: next(checks)),
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(MAIN_PATH), "exec"), namespace)

        self.assertTrue(namespace["_wait_for_previous_instance"](20))
        self.assertEqual(namespace["is_already_running"].call_count, 3)
        self.assertEqual(namespace["time"].sleep.call_count, 2)

    def test_malformed_parent_pid_does_not_consume_next_option(self):
        tree = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))
        function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "_consume_launch_parent_pid"
        )
        argv = ["refactored_main.py", "--clipflow-launch-parent-pid", "--safe-mode"]
        namespace = {
            "_LAUNCH_PARENT_ARG": "--clipflow-launch-parent-pid",
            "sys": SimpleNamespace(argv=argv),
        }
        exec(
            compile(ast.Module(body=[function], type_ignores=[]), str(MAIN_PATH), "exec"),
            namespace,
        )

        self.assertEqual(namespace["_consume_launch_parent_pid"](), 0)
        self.assertEqual(argv, ["refactored_main.py", "--safe-mode"])


if __name__ == "__main__":
    unittest.main()
