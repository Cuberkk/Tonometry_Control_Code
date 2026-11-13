"""Test sample selection without reading measurements or running analysis."""
import tempfile
import unittest
from pathlib import Path

from notebook_samples import select_sample_dirs


class SampleSelectionTests(unittest.TestCase):
    def test_single_and_batch_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            samples = [root / f'sample_{number:02d}' for number in (1, 2, 3)]
            for sample in samples:
                sample.mkdir()
            self.assertEqual(select_sample_dirs(samples[2], None), [samples[2]])
            self.assertEqual(select_sample_dirs(samples[2], [p.name for p in samples]), samples)
            self.assertEqual(select_sample_dirs(samples[0], ['sample_02']), [samples[1]])
            legacy = root / 'Hoang_Collected_Data'
            legacy.mkdir()
            self.assertEqual(select_sample_dirs(legacy, None), [legacy])
            with self.assertRaises(ValueError):
                select_sample_dirs(legacy, ['sample_01'])
            with self.assertRaises(FileNotFoundError):
                select_sample_dirs(samples[0], ['sample_04'])
            for names in ([], 'sample_01', ['sample_01', 'sample_01'],
                          ['sample_01/../sample_02'], ['../sample_01'], [None]):
                with self.subTest(names=names), self.assertRaises(ValueError):
                    select_sample_dirs(samples[0], names)


if __name__ == '__main__':
    unittest.main()
