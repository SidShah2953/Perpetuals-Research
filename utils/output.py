"""Helpers for standardised output directory layout."""

import inspect
import os
import re


def _filename_to_folder(stem: str) -> str:
    """Convert a filename stem like 'phase1A' into a title like 'Phase 1A'."""
    phase_number = stem[len("phase"):]
    phase_number = phase_number.upper()
    phase_number.replace("-", " - ")

    return f"Phase {phase_number}"


def setup_output_directory(output_root: str = "output") -> str:
    """Create and return a subfolder under *output_root* named after the calling script.

    Example::

        # in phase1B.py
        OUTPUT_DIR = setup_output_directory()  # -> "output/Phase 1B"
    """
    caller_frame = inspect.stack()[1]
    caller_file = os.path.basename(caller_frame.filename)
    stem = os.path.splitext(caller_file)[0]

    folder_name = _filename_to_folder(stem)
    path = os.path.join(output_root, folder_name)
    os.makedirs(path, exist_ok=True)
    return path
