"""Offline transport tests: no SSH connection or remote command is issued."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    'pcm_ssh_bridge', Path(__file__).with_name('ssh-bridge-administration-v1-20261022.py'))
BRIDGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BRIDGE)


class FakeInput(io.StringIO):
    def __init__(self, fail=None):
        super().__init__()
        self.fail = fail

    def write(self, value):
        if self.fail == 'write':
            raise BrokenPipeError('SSH stdin closed')
        return super().write(value)

    def flush(self):
        if self.fail == 'flush':
            raise BrokenPipeError('SSH disconnected after buffered write')
        return super().flush()


class FakeProcess:
    def __init__(self, output='', fail=None, exit_code=0):
        self.stdin = FakeInput(fail)
        self.stdout = io.StringIO(output)
        self.exit_code = exit_code

    def poll(self):
        return self.exit_code


class BridgeTests(unittest.TestCase):
    def prepare(self, directory):
        root = Path(directory)
        for name in ('requests', 'sent', 'completed', 'outputs'):
            (root / name).mkdir()
        return root

    def test_broken_console_keeps_draining_and_records_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            process = FakeProcess('__PCM_BEGIN__001\nretained output\n__PCM_END__001:7\n')
            disconnected = threading.Event()
            with patch('builtins.print', side_effect=BrokenPipeError('console closed')):
                BRIDGE.read_output(process, root, disconnected)
            self.assertTrue(disconnected.is_set())
            self.assertEqual(process.stdout.read(), '')
            self.assertEqual((root / 'outputs/001.txt').read_text(), 'retained output\n')
            receipt = json.loads((root / 'completed/001.json').read_text())
            self.assertEqual(receipt['exit_code'], 7)
            self.assertFalse(list(root.rglob('*.tmp-*')))

    def test_disconnect_closes_output_and_marks_active_request_ambiguous(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            process = FakeProcess('__PCM_BEGIN__002\npartial output\n', exit_code=255)
            disconnected, opened = threading.Event(), []
            original_open = Path.open

            def track_open(path, *args, **kwargs):
                handle = original_open(path, *args, **kwargs)
                if path == root / 'outputs/002.txt':
                    opened.append(handle)
                return handle

            with patch.object(Path, 'open', track_open), patch('builtins.print'):
                BRIDGE.read_output(process, root, disconnected)
            self.assertTrue(opened[0].closed)
            self.assertTrue(disconnected.is_set())
            self.assertFalse((root / 'completed/002.json').exists())
            self.assertEqual((root / 'outputs/002.txt').read_text(), 'partial output\n')
            transport = json.loads((root / 'sent/002.transport.json').read_text())
            self.assertEqual(transport['state'], 'transmission_outcome_unknown')
            record = json.loads((root / 'DISCONNECTED.json').read_text())
            self.assertEqual(record['ambiguous_request'], '002')
            self.assertEqual(record['ssh_exit'], 255)

    def test_write_or_flush_failure_is_durable_and_cannot_be_replayed(self):
        for failure in ('write', 'flush'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = self.prepare(directory)
                request = root / 'requests/003.sh'
                request.write_text('printf safe\n')
                process = FakeProcess(fail=failure, exit_code=255)
                with self.assertRaises(BRIDGE.TransportError):
                    BRIDGE.transmit_request(process, root, request)
                self.assertFalse(request.exists())
                self.assertEqual((root / 'sent/003.sh').read_text(), 'printf safe\n')
                transport = json.loads((root / 'sent/003.transport.json').read_text())
                self.assertEqual(transport['state'], 'transmission_outcome_unknown')
                record = json.loads((root / 'DISCONNECTED.json').read_text())
                self.assertEqual(record['reason'], 'stdin_write_failed')
                request.write_text('printf replacement\n')
                with self.assertRaisesRegex(RuntimeError, 'Duplicate request'):
                    BRIDGE.transmit_request(FakeProcess(), root, request)
                self.assertTrue(request.exists())
                self.assertEqual((root / 'sent/003.sh').read_text(), 'printf safe\n')

    def test_completed_id_refused_before_write_or_request_move(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            (root / 'completed/004.json').write_text('{"exit_code": 0}')
            request = root / 'requests/004.sh'
            request.write_text('printf duplicate\n')
            process = FakeProcess()
            with self.assertRaisesRegex(RuntimeError, 'Duplicate request'):
                BRIDGE.transmit_request(process, root, request)
            self.assertTrue(request.exists())
            self.assertEqual(process.stdin.getvalue(), '')
            self.assertFalse((root / 'sent/004.sh').exists())

    def test_successful_send_stays_unconfirmed_until_marker_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            request = root / 'requests/005.sh'
            request.write_text('exit 0\n')
            process = FakeProcess()
            self.assertEqual(BRIDGE.transmit_request(process, root, request), '005')
            self.assertIn('__PCM_BEGIN__005', process.stdin.getvalue())
            self.assertIn('/usr/bin/timeout --signal=TERM --kill-after=10s 180s /bin/bash -l 2>&1', process.stdin.getvalue())
            self.assertIn('/bin/bash -l 2>&1', process.stdin.getvalue())
            transport = json.loads((root / 'sent/005.transport.json').read_text())
            self.assertEqual(transport['state'], 'sent_completion_unconfirmed')
            self.assertEqual(transport['request_timeout_seconds'], 180)
            self.assertTrue(transport['login_shell'])
            self.assertFalse((root / 'completed/005.json').exists())

    def test_reader_error_sets_disconnect_and_preserves_partial_output(self):
        class FailingOutput:
            def __iter__(self):
                yield '__PCM_BEGIN__006\n'
                yield 'written before failure\n'
                raise OSError('SSH stdout read failed')

        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            process = FakeProcess(exit_code=None)
            process.stdout = FailingOutput()
            disconnected = threading.Event()
            with patch('builtins.print'):
                BRIDGE.read_output(process, root, disconnected)
            self.assertTrue(disconnected.is_set())
            self.assertEqual((root / 'outputs/006.txt').read_text(), 'written before failure\n')
            record = json.loads((root / 'DISCONNECTED.json').read_text())
            self.assertEqual(record['reason'], 'reader_failed')
            self.assertIn('SSH stdout read failed', record['error'])

    def test_strict_host_key_and_credentials_stderr_defaults_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            process = FakeProcess()
            with patch('sys.argv', ['ssh_bridge.py', '--root', directory]), \
                    patch.object(BRIDGE.subprocess, 'Popen', return_value=process) as popen, \
                    patch('builtins.print'):
                self.assertEqual(BRIDGE.main(), 0)
            command = popen.call_args.args[0]
            self.assertIn('StrictHostKeyChecking=yes', command)
            self.assertIn('pwa209@narval.alliancecan.ca', command)
            self.assertEqual(command[-1], '/bin/bash -l')
            self.assertIsNone(popen.call_args.kwargs['stderr'])

    def test_custom_timeout_wraps_nonlogin_request_without_changing_saved_script(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            request = root / 'requests/007.sh'
            source = 'printf "request bytes preserved\\n"\n'
            request.write_text(source)
            process = FakeProcess()
            BRIDGE.transmit_request(process, root, request, request_timeout=45, no_login_shell=True)
            command = process.stdin.getvalue()
            self.assertIn('base64 --decode | /usr/bin/timeout --signal=TERM --kill-after=10s 45s /bin/bash --noprofile --norc 2>&1', command)
            self.assertNotIn('/bin/bash -l', command)
            self.assertIn('pcm_exit=$?', command)
            self.assertNotIn('pcm_exit=124', command)
            self.assertEqual((root / 'sent/007.sh').read_text(), source)
            transport = json.loads((root / 'sent/007.transport.json').read_text())
            self.assertEqual(transport['request_timeout_seconds'], 45)
            self.assertFalse(transport['login_shell'])

    def test_timeout_and_kill_receipts_preserve_exit_codes_and_refuse_replay(self):
        for exit_code in (124, 137):
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as directory:
                root = self.prepare(directory)
                request = root / 'requests/008.sh'
                request.write_text('synthetic timeout fixture\n')
                BRIDGE.transmit_request(FakeProcess(), root, request)
                process = FakeProcess(f'__PCM_BEGIN__008\npartial diagnostic output\n__PCM_END__008:{exit_code}\n')
                with patch('builtins.print'):
                    BRIDGE.read_output(process, root, threading.Event())
                receipt_path = root / 'completed/008.json'
                old_receipt = receipt_path.read_bytes()
                self.assertEqual(json.loads(old_receipt)['exit_code'], exit_code)
                self.assertEqual((root / 'outputs/008.txt').read_text(), 'partial diagnostic output\n')
                request.write_text('replacement must not execute\n')
                retry = FakeProcess()
                with self.assertRaisesRegex(RuntimeError, 'Duplicate request'):
                    BRIDGE.transmit_request(retry, root, request)
                self.assertTrue(request.exists())
                self.assertEqual(retry.stdin.getvalue(), '')
                self.assertEqual(receipt_path.read_bytes(), old_receipt)

    def test_invalid_request_timeout_refused_before_transmission(self):
        for timeout in (0, -1, 1.5, '180', True):
            with self.subTest(timeout=timeout), tempfile.TemporaryDirectory() as directory:
                root = self.prepare(directory)
                request = root / 'requests/009.sh'
                request.write_text('exit 0\n')
                process = FakeProcess()
                with self.assertRaisesRegex(ValueError, 'positive integer'):
                    BRIDGE.transmit_request(process, root, request, request_timeout=timeout)
                self.assertTrue(request.exists())
                self.assertEqual(process.stdin.getvalue(), '')
                self.assertFalse(list((root / 'sent').iterdir()))

    def test_cli_nonlogin_shell_and_timeout_reach_initial_and_request_shells(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.prepare(directory)
            (root / 'requests/010.sh').write_text('exit 0\n')
            process = FakeProcess()
            polls = iter((None, None, 0, 0, 0))
            process.poll = lambda: next(polls, 0)
            with patch('sys.argv', ['ssh_bridge.py', '--root', directory, '--request-timeout', '42', '--no-login-shell']), \
                    patch.object(BRIDGE.subprocess, 'Popen', return_value=process) as popen, \
                    patch.object(BRIDGE.threading, 'Thread'), patch.object(BRIDGE.time, 'sleep'), \
                    patch.object(BRIDGE, 'transmit_request', wraps=BRIDGE.transmit_request) as transmit, \
                    patch('builtins.print'):
                self.assertEqual(BRIDGE.main(), 0)
            command = popen.call_args.args[0]
            self.assertEqual(command[-1], '/bin/bash --noprofile --norc')
            self.assertIn('StrictHostKeyChecking=yes', command)
            self.assertIsNone(popen.call_args.kwargs['stderr'])
            self.assertEqual(transmit.call_args.kwargs, {'request_timeout': 42, 'no_login_shell': True})
            transport = json.loads((root / 'sent/010.transport.json').read_text())
            self.assertEqual(transport['request_timeout_seconds'], 42)
            self.assertFalse(transport['login_shell'])

    def test_cli_rejects_nonpositive_timeout_before_ssh_start(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch('sys.argv', ['ssh_bridge.py', '--root', directory, '--request-timeout', '0']), \
                    patch.object(BRIDGE.subprocess, 'Popen') as popen, patch('sys.stderr', new_callable=io.StringIO):
                with self.assertRaises(SystemExit) as failure:
                    BRIDGE.main()
            self.assertEqual(failure.exception.code, 2)
            popen.assert_not_called()


if __name__ == '__main__':
    unittest.main()


def test_extended_administrative_deadline_keeps_system_paths_and_no_replay():
    with tempfile.TemporaryDirectory() as directory:
        root = BridgeTests().prepare(directory)
        request = root/'requests/extended1800.sh'
        request.write_text('exit 0\n')
        process = FakeProcess()
        BRIDGE.transmit_request(process, root, request, request_timeout=1800)
        assert '/usr/bin/timeout --signal=TERM --kill-after=10s 1800s /bin/bash -l' in process.stdin.getvalue()
        assert json.loads((root/'sent/extended1800.transport.json').read_text())['request_timeout_seconds'] == 1800
        request.write_text('exit 99\n')
        with unittest.TestCase().assertRaisesRegex(RuntimeError, 'Duplicate request'):
            BRIDGE.transmit_request(FakeProcess(), root, request, request_timeout=1800)
