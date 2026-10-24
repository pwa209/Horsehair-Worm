"""Native-read and allocation guard contracts with synthetic scheduler records."""
from pathlib import Path
from unittest import mock
import importlib.util
import json
import os
import subprocess
import sys
import unittest

AREA = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('verification_runner_tests', AREA/'run_native_reconciliation.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class NativeReadTests(unittest.TestCase):
    def test_original_record_ignores_batch_and_extern(self):
        raw = ('6865674.batch|FAILED|1:0|18500|1||def-ptewarie_cpu|start|end\n'
               '6865674|FAILED|1:0|18500|1|pwa209|def-ptewarie_cpu|start|end\n')
        with mock.patch.object(runner, 'command', return_value=raw):
            value, observed = runner.original_native('6865674')
        self.assertEqual(value['job_id'], '6865674')
        self.assertEqual(value['elapsed_seconds'], 18500)
        self.assertEqual(value['owner'], 'pwa209')
        self.assertEqual(observed, raw)

    def test_missing_parent_not_inferred_from_batch(self):
        with mock.patch.object(runner, 'command', return_value='6865674.batch|FAILED|1:0|1|1||||\n'):
            with self.assertRaises(ValueError):
                runner.original_native('6865674')

    def test_duplicate_parent_refused(self):
        raw = '6865674|FAILED|1:0|18500|1|pwa209|def-ptewarie_cpu|start|end\n'
        with mock.patch.object(runner, 'command', return_value=raw*2):
            with self.assertRaises(ValueError):
                runner.original_native('6865674')

    def test_truncated_native_refused(self):
        with mock.patch.object(runner, 'command', return_value='6865674|FAILED|1:0\n'):
            with self.assertRaises(ValueError):
                runner.original_native('6865674')

    def test_noninteger_native_allocation_refused(self):
        with mock.patch.object(runner, 'command', return_value='6865674|FAILED|1:0|nan|1|pwa209|a|s|e\n'):
            with self.assertRaises(ValueError):
                runner.original_native('6865674')

    def test_native_read_failure_not_empty_success(self):
        reply = subprocess.CompletedProcess(['sacct'], 1, '', 'temporarily unavailable')
        with mock.patch.object(runner.subprocess, 'run', return_value=reply):
            with self.assertRaises(ValueError):
                runner.command(['sacct'])

    def test_native_read_timeout_not_retried(self):
        with mock.patch.object(runner.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['sacct'], 45)) as call:
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.command(['sacct'])
            self.assertEqual(call.call_count, 1)

    def test_finite_duration_day_and_hour_formats(self):
        self.assertEqual(runner.duration('02:00:00'), 7200)
        self.assertEqual(runner.duration('4-00:00:00'), 345600)

    def test_unlimited_or_malformed_duration_refused(self):
        for text in ['UNLIMITED', 'N/A', '', '2 hours', 'nan']:
            with self.subTest(value=text), self.assertRaises(ValueError):
                runner.duration(text)


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(NativeReadTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    data = {'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors),
            'skipped': len(result.skipped), 'successful': result.wasSuccessful()}
    target = Path(os.environ.get('HHW_TEST_RESULT_DIR', str(AREA)))
    target.mkdir(parents=True, exist_ok=True)
    name = 'test-result.json' if os.environ.get('HHW_TEST_RESULT_DIR') else 'native-runner-local-test-result.json'
    (target/name).write_text(json.dumps(data, sort_keys=True, indent=2)+'\n', encoding='utf-8')
    raise SystemExit(0 if result.wasSuccessful() and not result.skipped else 1)
