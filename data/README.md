# Data Tugas 1

## Dataset yang Dipilih

| Item | Isi |
|---|---|
| Nama dataset | Indonesia Fire Hotspots (NASA FIRMS, daily) — file `hotspots.parquet` |
| Sumber | Kaggle: https://www.kaggle.com/datasets/danielaijdkx/indonesia-fire-hotspots-nasa-firms (data asli: NASA FIRMS, https://firms.modaps.eosdis.nasa.gov) |
| Versi yang dipakai | Kaggle dataset version 13 (diperbarui 2026-10-28) |
| Lisensi/ketentuan pakai | CC BY 4.0 — wajib mencantumkan kredit NASA FIRMS dan geoBoundaries |
| Ukuran | ±138 MB (Parquet terkompresi), ±6,08 juta baris |
| Periode data | 2012-01-20 s/d 2026-10-27 (Suomi NPP); 2018-04-01 s/d 2026-10-27 (NOAA-20) |
| Unit analisis | Satu baris = satu deteksi titik panas (piksel VIIRS 375 m) pada satu lintasan satelit, lengkap dengan provinsi |

### Mengapa dataset ini

- Memenuhi syarat ukuran (> 1 juta baris) dan berasal dari sumber yang diizinkan (Kaggle Indonesia).
- Memiliki dimensi **waktu** (tanggal + jam akuisisi) dan **ruang** (lat/lon + provinsi) sehingga cocok untuk analisis temporal/spasial di Milestone 3.
- Relevan dengan isu nyata Indonesia: kebakaran hutan dan lahan (*karhutla*), misalnya musim kebakaran 2015, 2019, dan 2023.

### Catatan cara membaca data (dari dokumentasi sumber)

- Dua satelit (Suomi NPP dan NOAA-20) mengamati api yang sama, sehingga **jumlah deteksi kedua satelit tidak boleh dijumlahkan**. Untuk tren sejak 2012 gunakan Suomi NPP saja.
- `frp` (*fire radiative power*, MW) mengukur intensitas api.
- Kolom `processing`: data hari-hari terakhir berasal dari *near-real-time* (NRT) dan akan diganti arsip final (SP) beberapa bulan kemudian.

## Cara Memperoleh Data

1. Login ke Kaggle, buka URL dataset di atas.
2. Unduh **hanya** file `hotspots.parquet` (tidak perlu `daily_province.csv` / `monthly_province.csv`).
   Alternatif lewat Kaggle CLI (butuh API token di `~/.kaggle/kaggle.json`):

   ```bash
   kaggle datasets download danielaijdkx/indonesia-fire-hotspots-nasa-firms \
       -f hotspots.parquet -p data/raw/
   ```

3. Simpan sebagai `data/raw/hotspots.parquet` tanpa mengubah isinya.
4. Notebook `01_data_profiling.ipynb` dan `02_data_cleaning.ipynb` sudah menunjuk ke path tersebut (`DATA_PATH`).

Karena dataset diperbarui harian, jumlah baris versi terbaru bisa sedikit lebih banyak daripada angka di laporan ini.

## Aturan Penyimpanan

- Jangan commit dataset mentah atau hasil olahan berukuran besar ke Git (`data/raw/*` dan `data/processed/*` sudah di-`.gitignore`).
- File pada `data/raw/` adalah data asli dan tidak boleh diubah.
- Hasil transformasi yang dapat direproduksi disimpan di `data/processed/` (dibuat oleh `02_data_cleaning.ipynb`).

## Atribusi

We acknowledge the use of data from NASA's Fire Information for Resource Management System (FIRMS), part of NASA's Earth Science Data and Information System (ESDIS). Batas provinsi: geoBoundaries (Runfola et al. 2020), CC BY 4.0. Dataset Kaggle disusun oleh pengguna `danielaijdkx`.
