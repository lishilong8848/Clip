"""Internal assistant worker, started and stopped with the ClipFlow backend."""
from __future__ import annotations

import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import socket
import sys
import threading
import time

from .protocol import PROTOCOL, PROJECT, InstanceLock, ServiceError, atomic_json, control_key, identity, migrate_accounts, process_stamp, protect_state_directory, read_json




async def serve(args):
    import win32api
    import win32process
    from upload_event_module.services.process_lifetime import close_child_process_job
    state = Path(args.state_root).absolute()
    if state.is_symlink() or state.is_junction():
        raise ServiceError('助手数据目录不可使用外部链接。', code='unsafe_state_directory')
    stopped_path = state / 'stopped.json'
    loop = asyncio.get_running_loop()
    host = server = listener = handler = None
    kill_timer = None
    stopping = threading.Event()
    stop_lock = threading.Lock()
    deadline_lock = threading.Lock()
    stop_reason = 'crash'
    win32process.SetPriorityClass(win32api.GetCurrentProcess(), win32process.BELOW_NORMAL_PRIORITY_CLASS)

    def stop(reason='manual'):
        nonlocal kill_timer, stop_reason
        # Arm exit and close owned children before waiting for startup disk I/O.
        with deadline_lock:
            if stopping.is_set() and reason != 'manual':
                return
            stopping.set()
            stop_reason = reason
            if host is not None:
                host.stop_reason = reason
            if kill_timer is None:
                # A blocked import/download cannot outlive an explicit stop.
                kill_timer = threading.Timer(4, lambda: os._exit(0))
                kill_timer.daemon = True
                kill_timer.start()
        try:
            close_child_process_job()
        finally:
            with stop_lock:
                if server is not None:
                    loop.call_soon_threadsafe(setattr, server, 'should_exit', True)
                atomic_json(stopped_path, {'reason': stop_reason,
                    'instance': host.instance if host else None, 'at': time.time()})

    def console_event(event):
        if event in (0, 1, 2, 5, 6):
            try:
                stop('manual' if event in (0, 1, 2) else 'system')
            except Exception:
                close_child_process_job()
            return True
        return False
    win32api.SetConsoleCtrlHandler(console_event, True)
    logger = logging.getLogger('openclaw_service')
    try:
        if (state / 'update-hold.json').is_file():
            print('[OpenClaw] Update is in progress; local configuration is preserved.', flush=True)
            return
        await asyncio.to_thread(protect_state_directory, state)
        if stopping.is_set():
            return
        state = state.resolve()
        import uvicorn
        from .server import Host, build_app
        if stopping.is_set():
            return
        await asyncio.to_thread(migrate_accounts, state)
        if stopping.is_set():
            return
        host = Host(project=args.project_root, state=state, runtime_root=args.runtime_root, key=control_key(state))
        logger.setLevel(logging.INFO)
        logs = state / 'logs'
        logs.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(logs / 'service.log', maxBytes=2_000_000, backupCount=3, encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
        logger.addHandler(handler)
        logger.propagate = False
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('127.0.0.1', 0))
        listener.listen(64)
        host.port = listener.getsockname()[1]
        app = build_app(host, request_stop=stop)
        async def startup():
            with stop_lock:
                if stopping.is_set():
                    host.stop_reason = stop_reason
                    server.should_exit = True
                    return
                atomic_json(state / 'service.json', {'identity': identity(args.project_root, state), 'instance': host.instance,
                    'pid': os.getpid(), 'process_stamp': process_stamp(os.getpid()), 'port': host.port, 'protocol': PROTOCOL,
                    'digest': host.digest, 'console_window': 0,
                    'parent_pid': args.parent_pid, 'parent_stamp': process_stamp(args.parent_pid)})
                stopped_path.unlink(missing_ok=True)
            print(f'[OpenClaw] Resident service is listening on loopback; pid={os.getpid()}, port={host.port}', flush=True)
            logger.info('host_ready pid=%d port=%d protocol=%d', os.getpid(), host.port, PROTOCOL)
            host.spawn(host.prepare())
            host.spawn(host.expire_reservations())
        app.add_event_handler('startup', startup)
        server = uvicorn.Server(uvicorn.Config(app, log_level='error', access_log=False,
            timeout_graceful_shutdown=2, lifespan='on'))
        await server.serve(sockets=[listener])
    finally:
        close_child_process_job()
        try:
            if host is not None:
                try:
                    await asyncio.wait_for(host.close(), 2)
                except asyncio.TimeoutError:
                    pass
                saved = read_json(state / 'service.json', {})
                if saved.get('instance') == host.instance:
                    (state / 'service.json').unlink(missing_ok=True)
                atomic_json(stopped_path, {'reason': host.stop_reason, 'instance': host.instance, 'at': time.time()})
                logger.info('host_stopped reason=%s', host.stop_reason)
        finally:
            win32api.SetConsoleCtrlHandler(console_event, False)
            if listener is not None:
                listener.close()
            if handler is not None:
                logger.removeHandler(handler)
                handler.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='ClipFlow internal OpenClaw worker')
    parser.add_argument('--parent-pid', type=int, required=True)
    parser.add_argument('--project-root', type=Path, default=PROJECT)
    parser.add_argument('--state-root', type=Path)
    parser.add_argument('--runtime-root', type=Path)
    args = parser.parse_args(argv)
    if args.state_root is None:
        args.state_root = args.project_root / 'bin/data/lighthouse_openclaw'
    if os.name != 'nt':
        print('[OpenClaw] This assistant worker requires Windows.', flush=True)
        return 1
    from upload_event_module.services.process_lifetime import start_parent_exit_watchdog
    if not start_parent_exit_watchdog(args.parent_pid):
        print('[OpenClaw] Parent process is no longer available; worker did not start.', flush=True)
        return 1
    try:
        with InstanceLock(args.project_root, args.state_root):
            asyncio.run(serve(args))
    except ServiceError as exc:
        print('[OpenClaw] ' + str(exc), flush=True)
        return 0 if exc.code == 'already_running' else 1
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        print('[OpenClaw] Startup did not complete (' + type(exc).__name__ + '); no business data was changed.', flush=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
