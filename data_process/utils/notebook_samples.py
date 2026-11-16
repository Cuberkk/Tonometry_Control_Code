"""Sample directory selection shared by preprocessing and fitting notebooks."""
from pathlib import Path


def select_sample_dirs(single_sample_dir, sample_names):
    """Select the configured directory, all sibling samples, or named samples."""
    single_sample_dir = Path(single_sample_dir)
    if sample_names is None:
        directories = [single_sample_dir]
    else:
        if sample_names != 'all' and (not isinstance(sample_names, list) or not sample_names
                or any(not isinstance(name, str) or not name.startswith('sample_')
                       or Path(name).name != name for name in sample_names)
                or len(sample_names) != len(set(sample_names))):
            raise ValueError("SAMPLE_NAMES must be None, 'all', or a nonempty list of unique sample directory names.")
        if not single_sample_dir.name.startswith('sample_'):
            raise ValueError('SAMPLE_NAMES requires a dataset path pointing to a sample_* directory.')
        directories = (sorted(path for path in single_sample_dir.parent.glob('sample_*') if path.is_dir())
                       if sample_names == 'all' else
                       [single_sample_dir.parent / name for name in sample_names])
        if not directories:
            raise FileNotFoundError(f'No sample_* directories found in {single_sample_dir.parent}')
    for directory in directories:
        if not directory.is_dir():
            raise FileNotFoundError(f'Sample directory not found: {directory}')
    return directories
