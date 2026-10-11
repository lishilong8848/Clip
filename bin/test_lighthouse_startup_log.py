"""Startup visibility without forwarding raw backend messages or credentials."""
import contextlib
import io
import json
import os
import socket
import sys
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from .lan_bitable_template_portal import lighthouse_startup_log as logs


class StartupLogTests(unittest.TestCase):
    def test_real_emit_render_contract(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            logs.emit('gateway_starting', account='aabbccdd', pid=123, port=18780)
        rendered = logs.render(output.getvalue().rstrip(), backend_pid=os.getpid())
        self.assertIn('正在启动共享常驻网关', rendered)
        self.assertIn('pid=123', rendered)

    def test_raw_errors_and_credentials_are_never_forwarded(self):
        for value in (
            'Exception: synthetic-password',
            logs.PREFIX + json.dumps({'stage':'gateway_ready', 'api_key':'synthetic-secret'}),
            logs.PREFIX + json.dumps({'stage':'gateway_ready', 'error':'Bearer synthetic-key'}),
            logs.PREFIX + json.dumps({'stage':'unknown'}),
            logs.PREFIX + json.dumps({'stage':['preparing']}),
            logs.PREFIX + json.dumps({'stage':'gateway_ready', 'pid':True}),
            logs.PREFIX + json.dumps({'stage':'gateway_ready', 'elapsed_ms':float('nan')}),
        ):
            self.assertIsNone(logs.render(value))

    def test_other_backend_pid_is_refused(self):
        line = logs.PREFIX + json.dumps({'stage':'preparing', 'backend_pid':123})
        self.assertIsNone(logs.render(line, backend_pid=456))

    def test_logging_problem_does_not_break_startup(self):
        with patch('builtins.print', side_effect=OSError('synthetic')):
            logs.emit('preparing')
        logs.emit('gateway_ready', api_key='synthetic-secret')
        logs.emit('gateway_ready', error=object())

    def test_new_lines_are_relayed_once_partial_lines_and_large_data_safe(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'backend.log'
            old = logs.PREFIX + json.dumps({'stage':'runtime_ready', 'backend_pid':123}) + '\n'
            path.write_text(old, encoding='utf-8')
            offset = path.stat().st_size
            stopped = threading.Event()
            received = []
            worker = threading.Thread(target=logs.relay_file, args=(path,offset,stopped,123), kwargs={'sink':received.append})
            worker.start()
            try:
                line = logs.PREFIX + json.dumps({'stage':'gateway_ready', 'backend_pid':123, 'pid':800}) + '\n'
                with path.open('ab') as stream:
                    stream.write(b'x' * 10000 + b'\n')
                    stream.write(line[:20].encode())
                time.sleep(.7)
                self.assertEqual(received, [])
                with path.open('ab') as stream:
                    stream.write(line[20:].encode())
                deadline = time.monotonic() + 2
                while not received and time.monotonic() < deadline:
                    time.sleep(.05)
                self.assertEqual(len(received), 1)
                self.assertIn('网关已就绪', received[0])
                time.sleep(.6)
                self.assertEqual(len(received), 1)
            finally:
                stopped.set()
                worker.join(2)
            self.assertFalse(worker.is_alive())

    def test_controller_reuses_one_worker_and_stop_joins_it(self):
        from clipflow_backend.process_controller import BackendProcessPortalController
        controller = BackendProcessPortalController()
        controller._process = Mock(pid=123)
        controller._process.poll.return_value = 0
        controller._process_log_file = io.StringIO()
        with patch('lan_bitable_template_portal.lighthouse_startup_log.relay_file', side_effect=lambda *a: a[2].wait()):
            controller._start_startup_log_relay(0)
            worker = controller._startup_log_thread
            controller._start_startup_log_relay(0)
            self.assertIs(worker,controller._startup_log_thread)
            with patch.object(controller,'_flush_active_delta_once'):
                controller.stop()
        self.assertFalse(worker.is_alive())

    def test_launcher_start_forwards_real_child_logs_without_starting_business_backend(self):
        from clipflow_backend.process_controller import BackendProcessPortalController
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0))
            port = listener.getsockname()[1]
        controller = BackendProcessPortalController(host='127.0.0.1',port=port)
        health = {'ok':True,'service':'clipflow_backend','runtime_root_hash':controller._runtime_root_hash,
                  'build_version':controller._build_version}
        fixture = """
import json,os,sys,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from lan_bitable_template_portal.lighthouse_startup_log import emit
health=json.loads(sys.argv[1]); port=int(sys.argv[sys.argv.index('--port')+1])
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*a): pass
 def reply(self):
  data=json.dumps(health).encode(); self.send_response(200)
  self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)))
  self.end_headers();self.wfile.write(data)
 def do_GET(self): self.reply()
 def do_POST(self):
  self.reply();threading.Thread(target=self.server.shutdown,daemon=True).start()
server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
emit('preparing');emit('runtime_ready',node='24.16.0',openclaw='2026.8.1')
print('synthetic-private-line-not-for-console',flush=True)
emit('awaiting_login')
server.serve_forever(poll_interval=.1);server.server_close()
"""
        with tempfile.TemporaryDirectory() as folder:
            log_path = Path(folder) / 'backend.log'
            output = io.StringIO()
            env = {**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parent), 'PYTHONIOENCODING':'utf-8'}
            command = ([sys.executable,'-B','-u','-c',fixture,json.dumps(health),'--host','127.0.0.1','--port',str(port)],env,folder)
            with patch.object(controller,'_backend_log_path',return_value=log_path), \
                 patch.object(controller,'_build_backend_command',return_value=command), \
                 patch.object(controller,'_ensure_bridge_threads'), \
                 patch('clipflow_backend.process_controller.register_child_process',return_value=True), \
                 contextlib.redirect_stdout(output):
                try:
                    controller.start()
                    deadline = time.monotonic() + 4
                    while '等待登录账号' not in output.getvalue() and time.monotonic() < deadline:
                        time.sleep(.05)
                    self.assertIn('运行环境已就绪',output.getvalue())
                    self.assertIn('等待登录账号',output.getvalue())
                    self.assertNotIn('synthetic-private-line',output.getvalue())
                    self.assertIn('synthetic-private-line',log_path.read_text(encoding='utf-8'))
                finally:
                    controller.stop()


if __name__ == '__main__':
    unittest.main()
