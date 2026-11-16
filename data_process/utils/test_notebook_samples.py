"""Sample selection checks using empty temporary directories only."""
import tempfile
import unittest
from pathlib import Path

from notebook_samples import select_sample_dirs


class SelectSampleDirsTests(unittest.TestCase):
    def test_selection_modes(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            samples = [root / name for name in ('sample_01', 'sample_02', 'sample_03')]
            for sample in samples:
                sample.mkdir()
            (root / 'other_data').mkdir()
            (root / 'sample_file').touch()

            self.assertEqual(select_sample_dirs(samples[0], None), [samples[0]])
            self.assertEqual(select_sample_dirs(samples[0], 'all'), samples)
            self.assertEqual(select_sample_dirs(samples[0], ['sample_03', 'sample_01']),
                             [samples[2], samples[0]])

            for selection in ('sample_01', [], ['sample_01', 'sample_01'], ['../sample_01']):
                with self.subTest(selection=selection), self.assertRaises(ValueError):
                    select_sample_dirs(samples[0], selection)
            with self.assertRaises(FileNotFoundError):
                select_sample_dirs(samples[0], ['sample_04'])
            with self.assertRaises(ValueError):
                select_sample_dirs(root / 'other_data', 'all')

    def test_all_without_samples(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            with self.assertRaises(FileNotFoundError):
                select_sample_dirs(Path(temporary_dir) / 'sample_01', 'all')


if __name__ == '__main__':
    unittest.main()
