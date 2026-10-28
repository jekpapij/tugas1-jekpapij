"""Lokasi file dan konstanta yang dipakai bersama oleh seluruh notebook."""

from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    """Cari root repository (folder yang berisi requirements.txt).

    Notebook bisa dijalankan dari folder `notebooks/` maupun dari root,
    jadi path tidak boleh bergantung pada working directory.
    """
    current = (start or Path.cwd()).resolve()
    for folder in (current, *current.parents):
        if (folder / "requirements.txt").exists() and (folder / "notebooks").is_dir():
            return folder
    raise FileNotFoundError("Root repository tidak ditemukan dari " + str(current))


PROJECT_ROOT = find_project_root()
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "hotspots.parquet"
CLEAN_PATH = PROJECT_ROOT / "data" / "processed" / "hotspots_clean.parquet"
FIGURES_DIR = PROJECT_ROOT / "output" / "figures"
