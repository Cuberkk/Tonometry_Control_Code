"""Sample directory selection shared by preprocessing and fitting notebooks."""
from pathlib import Path


def select_sample_dirs(single_sample_dir, sample_names):
    """Keep the configured directory, or select named samples beside it."""
    single_sample_dir = Path(single_sample_dir)
    if sample_names is None:
        directories = [single_sample_dir]
    else:
        if (not isinstance(sample_names, list) or not sample_names
                or any(not isinstance(name, str) or not name.startswith('sample_')
                       or Path(name).name != name for name in sample_names)
                or len(sample_names) != len(set(sample_names))):
            raise ValueError('SAMPLE_NAMES must be None or a nonempty list of unique sample directory names.')
        if not single_sample_dir.name.startswith('sample_'):
            raise ValueError('SAMPLE_NAMES requires a dataset path pointing to a sample_* directory.')
        directories = [single_sample_dir.parent / name for name in sample_names]
    for directory in directories:
        if not directory.is_dir():
            raise FileNotFoundError(f'Sample directory not found: {directory}')
    return directories
