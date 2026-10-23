"""Bound tiny numerical fixtures to one CPU thread; full jobs use explicit Slurm CPUs."""
import os
os.environ.setdefault('OMP_NUM_THREADS','1')
os.environ.setdefault('MKL_NUM_THREADS','1')
os.environ.setdefault('OPENBLAS_NUM_THREADS','1')


def pytest_sessionstart(session):
    try:
        import torch
        torch.set_num_threads(1)
    except ImportError:
        pass

