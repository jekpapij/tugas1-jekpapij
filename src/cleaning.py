from pathlib import Path

import duckdb
import polars as pl

KEY_COLUMNS = ["satellite", "acq_datetime", "latitude", "longitude"]

# Batas kotak (bounding box) wilayah Indonesia, dengan sedikit margin.
LAT_RANGE = (-11.5, 6.5)
LON_RANGE = (94.5, 141.5)

# Saturasi kanal I4 VIIRS: nilai 367 K adalah batas atas sensor, bukan suhu sebenarnya.
TI4_SATURATION = 367.0
# Nilai bright_ti5 <= 150 K tidak realistis untuk permukaan bumi (nilai pengisi/anomali sensor).
TI5_MIN_VALID = 150.0

VALID_SATELLITE = ["Suomi NPP", "NOAA-20"]
VALID_CONFIDENCE = ["low", "nominal", "high"]
VALID_DAYNIGHT = ["D", "N"]
VALID_PROCESSING = ["SP", "NRT"]

# Nama provinsi pada dataset (geoBoundaries, bahasa Inggris, 34 provinsi sebelum
# pemekaran Papua 2022) -> nama resmi bahasa Indonesia, zona waktu, dan pulau.
PROVINSI = pl.DataFrame(
    [
        ("Aceh", "Aceh", "WIB", "Sumatera"),
        ("North Sumatra", "Sumatera Utara", "WIB", "Sumatera"),
        ("West Sumatra", "Sumatera Barat", "WIB", "Sumatera"),
        ("Riau", "Riau", "WIB", "Sumatera"),
        ("Riau Islands", "Kepulauan Riau", "WIB", "Sumatera"),
        ("Jambi", "Jambi", "WIB", "Sumatera"),
        ("South Sumatra", "Sumatera Selatan", "WIB", "Sumatera"),
        ("Bangka-Belitung Islands", "Kepulauan Bangka Belitung", "WIB", "Sumatera"),
        ("Bengkulu", "Bengkulu", "WIB", "Sumatera"),
        ("Lampung", "Lampung", "WIB", "Sumatera"),
        ("Jakarta Special Capital Region", "DKI Jakarta", "WIB", "Jawa"),
        ("West Java", "Jawa Barat", "WIB", "Jawa"),
        ("Banten", "Banten", "WIB", "Jawa"),
        ("Central Java", "Jawa Tengah", "WIB", "Jawa"),
        ("Special Region of Yogyakarta", "DI Yogyakarta", "WIB", "Jawa"),
        ("East Java", "Jawa Timur", "WIB", "Jawa"),
        ("Bali", "Bali", "WITA", "Bali & Nusa Tenggara"),
        ("West Nusa Tenggara", "Nusa Tenggara Barat", "WITA", "Bali & Nusa Tenggara"),
        ("East Nusa Tenggara", "Nusa Tenggara Timur", "WITA", "Bali & Nusa Tenggara"),
        ("West Kalimantan", "Kalimantan Barat", "WIB", "Kalimantan"),
        ("Central Kalimantan", "Kalimantan Tengah", "WIB", "Kalimantan"),
        ("South Kalimantan", "Kalimantan Selatan", "WITA", "Kalimantan"),
        ("East Kalimantan", "Kalimantan Timur", "WITA", "Kalimantan"),
        ("North Kalimantan", "Kalimantan Utara", "WITA", "Kalimantan"),
        ("North Sulawesi", "Sulawesi Utara", "WITA", "Sulawesi"),
        ("Gorontalo", "Gorontalo", "WITA", "Sulawesi"),
        ("Central Sulawesi", "Sulawesi Tengah", "WITA", "Sulawesi"),
        ("West Sulawesi", "Sulawesi Barat", "WITA", "Sulawesi"),
        ("South Sulawesi", "Sulawesi Selatan", "WITA", "Sulawesi"),
        ("Southeast Sulawesi", "Sulawesi Tenggara", "WITA", "Sulawesi"),
        ("Maluku", "Maluku", "WIT", "Maluku & Papua"),
        ("North Maluku", "Maluku Utara", "WIT", "Maluku & Papua"),
        ("Papua", "Papua", "WIT", "Maluku & Papua"),
        ("West Papua", "Papua Barat", "WIT", "Maluku & Papua"),
    ],
    schema=["province", "provinsi", "zona_waktu", "pulau"],
    orient="row",
)
UTC_OFFSET_JAM = {"WIB": 7, "WITA": 8, "WIT": 9}


def standardize(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rapikan tipe dan teks, lalu tambahkan atribut wilayah berbahasa Indonesia."""
    return (
        lf.with_columns(
            pl.col("acq_date").cast(pl.Date),
            pl.col("satellite", "province", "daynight", "processing").str.strip_chars(),
            pl.col("confidence").str.strip_chars().str.to_lowercase(),
        )
        .join(PROVINSI.lazy(), on="province", how="left")
        .with_columns(
            (
                pl.col("acq_datetime")
                + pl.duration(
                    hours=pl.col("zona_waktu").replace_strict(UTC_OFFSET_JAM, return_dtype=pl.Int64)
                )
            ).alias("acq_datetime_lokal"),
        )
    )


def dedup_with_duckdb(
    connection: duckdb.DuckDBPyConnection,
    raw_path: Path,
    out_path: Path,
) -> int:
    """Tahap 1 (DuckDB): buang NRT yang sudah digantikan SP dan deduplikasi.

    - Menurut dokumentasi sumber, data NRT akan digantikan arsip final SP.
      Baris NRT pada tanggal yang sudah tercakup SP dibuang agar deteksi
      tidak terhitung dua kali.
    - Satu deteksi = kombinasi unik satelit, waktu akuisisi, dan koordinat
      piksel (`KEY_COLUMNS`). Jika ada duplikat, SP diprioritaskan.

    Deduplikasi 6 juta baris dengan `unique()` Polars di laptop RAM 4 GB
    terkena out-of-memory, sedangkan DuckDB bisa dibatasi memorinya dan
    otomatis spill ke disk. Mengembalikan jumlah baris hasil.
    """
    key = ", ".join(KEY_COLUMNS)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"""
        COPY (
            WITH raw AS (SELECT * FROM read_parquet('{raw_path.as_posix()}')),
            sp AS (
                SELECT satellite, MAX(acq_date) AS sp_terakhir
                FROM raw WHERE processing = 'SP' GROUP BY satellite
            )
            SELECT DISTINCT ON ({key}) raw.*
            FROM raw LEFT JOIN sp USING (satellite)
            WHERE processing = 'SP' OR sp_terakhir IS NULL OR acq_date > sp_terakhir
            ORDER BY {key}, processing DESC  -- 'SP' > 'NRT'
        ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
        """
    )
    return connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path.as_posix()}')"
    ).fetchone()[0]


def log_frp_iqr_bounds(lf: pl.LazyFrame, k: float = 1.5) -> tuple[float, float]:
    """Batas outlier IQR pada skala log10(FRP).

    FRP sangat menceng ke kanan (median ~4 MW, maksimum ~2000 MW), sehingga IQR
    pada skala asli akan menandai terlalu banyak titik. Skala log lebih wajar.
    """
    q = (
        lf.filter(pl.col("frp") > 0)
        .select(
            pl.col("frp").log10().quantile(0.25).alias("q1"),
            pl.col("frp").log10().quantile(0.75).alias("q3"),
        )
        .collect()
        .row(0, named=True)
    )
    iqr = q["q3"] - q["q1"]
    return 10 ** (q["q1"] - k * iqr), 10 ** (q["q3"] + k * iqr)


IMPUTE_GROUP = ["satellite", "daynight", "province"]


def frp_group_medians(lf: pl.LazyFrame) -> pl.DataFrame:
    """Median FRP valid (> 0) per (satelit, siang/malam, provinsi) -> tabel kecil untuk imputasi."""
    return (
        lf.filter(pl.col("frp") > 0)
        .group_by(IMPUTE_GROUP)
        .agg(pl.col("frp").median().alias("frp_median_grup"))
        .collect()
    )


def handle_values(
    lf: pl.LazyFrame,
    frp_bounds: tuple[float, float],
    frp_medians: pl.DataFrame,
) -> pl.LazyFrame:
    """Tandai anomali, tangani nilai tidak valid, dan buat kolom turunan.

    - FRP = 0 tidak bermakna untuk deteksi api -> dianggap missing lalu diimputasi
      dengan median FRP pada kelompok (satelit, siang/malam, provinsi).
    - bright_ti5 <= 150 K -> dianggap missing (tidak diimputasi, tidak dipakai analisis).
    - Saturasi I4, I4 < I5, dan outlier FRP hanya DITANDAI (nilai asli dipertahankan,
      karena kebakaran besar memang menghasilkan FRP ekstrem).
    - frp_winsor: FRP yang di-clip ke batas IQR log, khusus untuk visualisasi/agregasi rata-rata.
    """
    batas_bawah, batas_atas = frp_bounds
    frp_valid = pl.when(pl.col("frp") > 0).then(pl.col("frp"))
    return (
        lf.with_columns(
            (pl.col("frp") <= 0).alias("frp_imputasi"),
            frp_valid.alias("frp"),
            pl.when(pl.col("bright_ti5") > TI5_MIN_VALID).then(pl.col("bright_ti5")).alias("bright_ti5"),
            (pl.col("bright_ti4") >= TI4_SATURATION).alias("ti4_saturasi"),
            (pl.col("bright_ti4") < pl.col("bright_ti5")).alias("anomali_ti4_lt_ti5"),
            (pl.col("confidence") != "low").alias("andal"),
        )
        .join(frp_medians.lazy(), on=IMPUTE_GROUP, how="left")
        .with_columns(pl.col("frp").fill_null(pl.col("frp_median_grup")))
        .with_columns(
            ((pl.col("frp") < batas_bawah) | (pl.col("frp") > batas_atas)).alias("frp_outlier"),
            pl.col("frp").clip(batas_bawah, batas_atas).alias("frp_winsor"),
            pl.col("acq_date").dt.year().alias("tahun"),
            pl.col("acq_date").dt.month().alias("bulan"),
        )
    )


def cast_categories(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Kolom berkardinalitas rendah -> Enum (hemat memori, nilai di luar daftar langsung error)."""
    return lf.with_columns(
        pl.col("satellite").cast(pl.Enum(VALID_SATELLITE)),
        pl.col("confidence").cast(pl.Enum(VALID_CONFIDENCE)),
        pl.col("daynight").cast(pl.Enum(VALID_DAYNIGHT)),
        pl.col("processing").cast(pl.Enum(VALID_PROCESSING)),
        pl.col("zona_waktu").cast(pl.Enum(["WIB", "WITA", "WIT"])),
        pl.col("pulau").cast(pl.Categorical),
        pl.col("provinsi").cast(pl.Categorical),
    )


OUTPUT_COLUMNS = [
    "acq_date", "acq_datetime", "acq_datetime_lokal", "tahun", "bulan",
    "satellite", "processing", "daynight",
    "latitude", "longitude", "province", "provinsi", "pulau", "zona_waktu",
    "confidence", "andal",
    "frp", "frp_winsor", "frp_outlier", "frp_imputasi",
    "bright_ti4", "bright_ti5", "ti4_saturasi", "anomali_ti4_lt_ti5",
]


def pipeline_cleaning(
    lf: pl.LazyFrame,
    frp_bounds: tuple[float, float],
    frp_medians: pl.DataFrame,
) -> pl.LazyFrame:
    """Tahap 2 (Polars): seluruh transformasi dalam satu rantai lazy.

    Statistik yang butuh satu pass penuh (batas IQR dan median imputasi)
    dihitung dulu sebagai tabel kecil lalu dipakai lewat join, bukan window
    function, sehingga pipeline bisa dieksekusi streaming dengan `sink_parquet`.
    """
    return (
        lf.pipe(standardize)
        .pipe(handle_values, frp_bounds=frp_bounds, frp_medians=frp_medians)
        .pipe(cast_categories)
        .select(OUTPUT_COLUMNS)
    )
