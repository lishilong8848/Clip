import concurrent.futures
import sys
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_auth import (  # noqa: E402
    AUTH_SESSION_TTL_SECONDS,
    PortalAuthManager,
    PortalAuthStateError,
)
from lan_bitable_template_portal.workbench_lite import (  # noqa: E402
    extract_workbench_lite_fragments,
)


@contextmanager
def auth_test_directory():
    managers = []
    manager_type = PortalAuthManager

    def create():
        manager = manager_type()
        managers.append(manager)
        return manager

    with tempfile.TemporaryDirectory() as tmp:
        with patch(__name__ + ".PortalAuthManager", side_effect=create):
            try:
                yield tmp
            finally:
                for manager in managers:
                    manager.shutdown()
                    manager._cleanup_executor.shutdown(
                        wait=True, cancel_futures=True
                    )
                    manager._state_store.shutdown_write_worker()


class PortalAuthPersistenceTests(unittest.TestCase):
    def test_oauth_state_survives_restart_and_is_consumed_once(self):
        with auth_test_directory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                state = "restart-safe-state"
                redirect_uri = "http://127.0.0.1:18766/api/auth/feishu/callback"
                first_manager = PortalAuthManager()
                first_manager._state_store.put_auth_oauth_state(
                    first_manager._secret_hash(state),
                    redirect_uri=redirect_uri,
                    next_path="/workbench-lite?scope=E&work_type=maintenance",
                    expires_at=time.time() + 600,
                )

                restarted_manager = PortalAuthManager()
                restarted_manager._exchange_login_code = lambda _code: {
                    "open_id": "ou_restart_user",
                    "name": "重启测试用户",
                }
                session_id, next_path = restarted_manager.complete_login(
                    code="valid-code",
                    state=state,
                    redirect_uri=redirect_uri,
                )

                self.assertTrue(session_id)
                self.assertEqual(
                    next_path,
                    "/workbench-lite?scope=E&work_type=maintenance",
                )
                with self.assertRaises(PortalAuthStateError):
                    restarted_manager.complete_login(
                        code="duplicate-code",
                        state=state,
                        redirect_uri=redirect_uri,
                    )

    def test_concurrent_oauth_callbacks_only_consume_state_once(self):
        with auth_test_directory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                state = "concurrent-state"
                redirect_uri = "http://127.0.0.1:18766/api/auth/feishu/callback"
                seed_manager = PortalAuthManager()
                seed_manager._state_store.put_auth_oauth_state(
                    seed_manager._secret_hash(state),
                    redirect_uri=redirect_uri,
                    next_path="/workbench-lite?scope=A",
                    expires_at=time.time() + 600,
                )
                managers = [PortalAuthManager(), PortalAuthManager()]
                for index, manager in enumerate(managers):
                    manager._exchange_login_code = lambda _code, idx=index: {
                        "open_id": f"ou_concurrent_{idx}",
                        "name": f"并发用户{idx}",
                    }

                def complete(index):
                    try:
                        session_id, next_path = managers[index].complete_login(
                            code=f"code-{index}",
                            state=state,
                            redirect_uri=redirect_uri,
                        )
                        return ("ok", session_id, next_path)
                    except PortalAuthStateError as exc:
                        return ("rejected", str(exc), "")

                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(complete, range(2)))

                self.assertEqual([item[0] for item in results].count("ok"), 1)
                self.assertEqual([item[0] for item in results].count("rejected"), 1)
                winner = next(item for item in results if item[0] == "ok")
                self.assertEqual(winner[2], "/workbench-lite?scope=A")

    def test_session_survives_restart_and_logout_revokes_it(self):
        with auth_test_directory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                session_id = "restart-safe-session"
                first_manager = PortalAuthManager()
                first_manager._state_store.put_auth_session(
                    first_manager._secret_hash(session_id),
                    {
                        "user": {
                            "open_id": "ou_restart_session",
                            "name": "会话测试用户",
                        },
                        "role": "building",
                        "allowed_scopes": ["E"],
                        "created_at": "2026-07-25 10:00:00",
                        "created_at_ts": time.time(),
                        "expires_at": time.time() + AUTH_SESSION_TTL_SECONDS,
                    },
                )

                restarted_manager = PortalAuthManager()
                session = restarted_manager.get_session(session_id)
                self.assertIsNotNone(session)
                self.assertEqual(
                    session["user"]["open_id"],
                    "ou_restart_session",
                )

                restarted_manager.clear_session(session_id)
                after_logout = PortalAuthManager()
                self.assertIsNone(after_logout.get_session(session_id))

    def test_expired_oauth_state_keeps_the_original_return_path(self):
        with auth_test_directory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                state = "expired-restart-state"
                manager = PortalAuthManager()
                manager._state_store.put_auth_oauth_state(
                    manager._secret_hash(state),
                    redirect_uri="http://127.0.0.1:18766/api/auth/feishu/callback",
                    next_path="/engineer/mop?scope=E",
                    expires_at=time.time() - 1,
                )

                restarted_manager = PortalAuthManager()
                try:
                    with self.assertRaises(PortalAuthStateError) as caught:
                        restarted_manager.complete_login(
                            code="expired-code",
                            state=state,
                            redirect_uri=(
                                "http://127.0.0.1:18766/api/auth/feishu/callback"
                            ),
                        )
                    self.assertEqual(caught.exception.next_path, "/engineer/mop?scope=E")
                finally:
                    restarted_manager._cleanup_executor.shutdown(wait=True)
                    manager._cleanup_executor.shutdown(wait=True)

    def test_disabling_user_revokes_a_persisted_session_immediately(self):
        with auth_test_directory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                session_id = "disabled-user-session"
                manager = PortalAuthManager()
                manager.upsert_permission_user(
                    open_id="ou_disabled_session",
                    name="禁用测试用户",
                    scopes=["E"],
                )
                manager._state_store.put_auth_session(
                    manager._secret_hash(session_id),
                    {
                        "user": {
                            "open_id": "ou_disabled_session",
                            "name": "禁用测试用户",
                        },
                        "role": "building",
                        "allowed_scopes": ["E"],
                        "expires_at": time.time() + AUTH_SESSION_TTL_SECONDS,
                    },
                )
                manager.upsert_permission_user(
                    open_id="ou_disabled_session",
                    name="禁用测试用户",
                    scopes=["E"],
                    enabled=False,
                )

                self.assertIsNone(PortalAuthManager().get_session(session_id))

    def test_teardown_waits_for_background_cleanup_before_removing_temp(
        self,
    ):
        # Deterministic regression: teardown must wait for the background
        # cleanup task (which holds the temp SQLite DB) before removing the
        # temp dir. Use Events, not sleeps; failures propagate via `errors`.
        cleanup_started = threading.Event()
        release_cleanup = threading.Event()
        wait_true_seen = threading.Event()
        cleanup_finished = threading.Event()
        observed: list[str] = []
        errors: list[BaseException] = []

        def run_body() -> None:
            try:
                with auth_test_directory() as tmp:
                    root = Path(tmp)
                    with patch(
                        "lan_bitable_template_portal.portal_auth.get_data_file_path",
                        side_effect=lambda name: str(root / name),
                    ):
                        manager = PortalAuthManager()
                        store = manager._state_store
                        original_cleanup = store.cleanup_auth_runtime
                        original_shutdown = manager._cleanup_executor.shutdown

                        def blocking_cleanup(*args, **kwargs):
                            observed.append("cleanup:started")
                            cleanup_started.set()
                            assert release_cleanup.wait(timeout=10), (
                                "release_cleanup timeout"
                            )
                            try:
                                return original_cleanup(*args, **kwargs)
                            finally:
                                observed.append("cleanup:finished")
                                cleanup_finished.set()

                        def patched_shutdown(wait=False, cancel_futures=False):
                            if wait:
                                wait_true_seen.set()
                                release_cleanup.wait(timeout=20)
                            return original_shutdown(
                                wait=wait, cancel_futures=cancel_futures
                            )

                        store.cleanup_auth_runtime = blocking_cleanup
                        manager._cleanup_executor.shutdown = patched_shutdown
                        with manager._lock:
                            manager._last_persistent_cleanup = time.time() - 120
                            now = time.time()
                            manager._cleanup_expired_locked(now)
                        # Wait for the submitted task to start BEFORE exiting
                        # the context so teardown's cancel_futures cannot
                        # cancel a not-yet-started task.
                        if not cleanup_started.wait(timeout=10):
                            raise AssertionError(
                                "cleanup task did not start before teardown"
                            )

                observed.append("teardown:complete")
            except BaseException as exc:
                errors.append(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(run_body)
            try:
                self.assertTrue(
                    cleanup_started.wait(timeout=10),
                    "background cleanup task did not start before teardown",
                )
                self.assertFalse(cleanup_finished.is_set())
                self.assertTrue(
                    wait_true_seen.wait(timeout=10),
                    "teardown did not wait on the cleanup executor (wait=True)",
                )
                self.assertFalse(cleanup_finished.is_set())
            finally:
                release_cleanup.set()
            future.result(timeout=15)

        self.assertEqual(
            errors,
            [],
            "run_body raised an error; see traceback of the first error",
        )
        self.assertEqual(
            observed,
            [
                "cleanup:started",
                "cleanup:finished",
                "teardown:complete",
            ],
        )


class WorkbenchFragmentTests(unittest.TestCase):
    def test_extracts_each_workbench_fragment_without_surrounding_page(self):
        html = "".join(
            f"before<!--LITE_FRAGMENT:{name}:START-->"
            f"<section id='{name}'>{name}</section>"
            f"<!--LITE_FRAGMENT:{name}:END-->after"
            for name in ("subtitle", "summary", "toolbar", "workspace", "detail")
        )

        fragments = extract_workbench_lite_fragments(html)

        self.assertEqual(set(fragments), {
            "subtitle",
            "summary",
            "toolbar",
            "workspace",
            "detail",
        })
        self.assertEqual(
            fragments["workspace"],
            "<section id='workspace'>workspace</section>",
        )


if __name__ == "__main__":
    unittest.main()
