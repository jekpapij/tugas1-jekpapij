"""Aturan validasi kualitas data berbasis DuckDB.

Setiap aturan berisi kondisi SQL yang bernilai TRUE untuk baris yang
MELANGGAR aturan (NULL dianggap tidak melanggar,
cek NULL dibuat sebagai aturan terpisah). Semua aturan dihitung
dalam satu kali scan tabel.
"""

from dataclasses import dataclass

import duckdb
import polars as pl


@dataclass(frozen=True)
class Rule:
    name: str
    dimension: str  # completeness / validity / uniqueness / consistency / accuracy
    violation_sql: str
    description: str


def run_rules(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    rules: list[Rule],
    unique_key: list[str] | None = None,
) -> pl.DataFrame:
    """Hitung jumlah pelanggaran tiap aturan pada `table`.

    `{table}` di dalam `violation_sql` diganti nama tabel (untuk subquery).
    Jika `unique_key` diisi, ditambahkan aturan keunikan: jumlah baris
    dikurangi jumlah kombinasi kunci yang berbeda.
    """
    select_parts = ["COUNT(*) AS __total"]
    for i, rule in enumerate(rules):
        kondisi = rule.violation_sql.format(table=table)
        select_parts.append(f"COUNT(*) FILTER (WHERE COALESCE(({kondisi}), FALSE)) AS r{i}")
    counts = connection.execute(
        f"SELECT {', '.join(select_parts)} FROM {table}"
    ).fetchone()
    total = counts[0]
    names = [r.name for r in rules]
    dims = [r.dimension for r in rules]
    descs = [r.description for r in rules]
    violations = [int(c) for c in counts[1:]]
    if unique_key:
        key = ", ".join(unique_key)
        distinct = connection.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT {key} FROM {table})"
        ).fetchone()[0]
        names.append("kunci_unik")
        dims.append("uniqueness")
        descs.append(f"Kombinasi ({key}) tidak boleh berulang")
        violations.append(int(total - distinct))
    return pl.DataFrame(
        {"aturan": names, "dimensi": dims, "deskripsi": descs, "pelanggaran": violations}
    ).with_columns(
        (pl.col("pelanggaran") / total * 100).round(4).alias("persen"),
        (pl.col("pelanggaran") == 0).alias("lulus"),
    )


def assert_all_pass(report: pl.DataFrame) -> None:
    """Gagalkan eksekusi notebook jika ada aturan yang dilanggar."""
    failed = report.filter(~pl.col("lulus"))
    if failed.height:
        raise AssertionError(
            "Validasi gagal:\n" + str(failed.select("aturan", "pelanggaran"))
        )


# ---------------------------------------------------------------------------
# Aturan khusus dataset hotspot NASA FIRMS
# ---------------------------------------------------------------------------

RAW_RULES = [
    Rule("kolom_kunci_lengkap", "completeness",
         "satellite IS NULL OR acq_datetime IS NULL OR latitude IS NULL OR longitude IS NULL",
         "Satelit, waktu, dan koordinat wajib terisi"),
    Rule("frp_terisi", "completeness", "frp IS NULL", "FRP wajib terisi"),
    Rule("frp_positif", "validity", "frp <= 0", "FRP deteksi api harus > 0 MW"),
    Rule("bright_ti5_wajar", "validity", "bright_ti5 <= 150",
         "Suhu kecerahan I5 <= 150 K tidak realistis"),
    Rule("koordinat_di_indonesia", "validity",
         "latitude NOT BETWEEN -11.5 AND 6.5 OR longitude NOT BETWEEN 94.5 AND 141.5",
         "Koordinat berada di kotak wilayah Indonesia"),
    Rule("satelit_valid", "validity", "satellite NOT IN ('Suomi NPP', 'NOAA-20')",
         "Hanya Suomi NPP / NOAA-20"),
    Rule("confidence_valid", "validity", "confidence NOT IN ('low', 'nominal', 'high')",
         "Confidence hanya low/nominal/high"),
    Rule("daynight_valid", "validity", "daynight NOT IN ('D', 'N')", "Siang/malam hanya D/N"),
    Rule("processing_valid", "validity", "processing NOT IN ('SP', 'NRT')",
         "Status pemrosesan hanya SP/NRT"),
    Rule("jam_sesuai_siang_malam", "consistency",
         "(daynight = 'D' AND hour(acq_datetime) NOT BETWEEN 2 AND 9)"
         " OR (daynight = 'N' AND hour(acq_datetime) NOT BETWEEN 14 AND 21)",
         "Lintasan siang ±02-09 UTC, malam ±14-21 UTC"),
    Rule("tanggal_sesuai_waktu", "consistency",
         "CAST(acq_date AS DATE) <> CAST(acq_datetime AS DATE)",
         "acq_date sama dengan tanggal acq_datetime"),
    Rule("nrt_tidak_tumpang_tindih_sp", "consistency",
         "processing = 'NRT' AND CAST(acq_date AS DATE) <= (SELECT MAX(CAST(s.acq_date AS DATE)) "
         "FROM {table} s WHERE s.processing = 'SP' AND s.satellite = {table}.satellite)",
         "NRT hanya untuk tanggal setelah arsip SP terakhir"),
]


def clean_rules(frp_bounds: tuple[float, float]) -> list[Rule]:
    """Aturan tambahan untuk kolom hasil cleaning."""
    lo, hi = frp_bounds
    return [
        Rule("provinsi_terpetakan", "completeness",
             "provinsi IS NULL OR zona_waktu IS NULL OR pulau IS NULL",
             "Setiap provinsi punya nama Indonesia, zona waktu, dan pulau"),
        Rule("waktu_lokal_konsisten", "consistency",
             "date_diff('hour', acq_datetime, acq_datetime_lokal) <> "
             "CASE zona_waktu WHEN 'WIB' THEN 7 WHEN 'WITA' THEN 8 ELSE 9 END",
             "Waktu lokal = UTC + 7/8/9 jam sesuai zona"),
        Rule("frp_winsor_dalam_batas", "validity",
             f"frp_winsor < {lo} OR frp_winsor > {hi}",
             "FRP winsorized berada di batas IQR log"),
        Rule("flag_outlier_konsisten", "consistency",
             f"frp_outlier <> (frp < {lo} OR frp > {hi})",
             "Flag outlier sesuai batas IQR log"),
        Rule("flag_andal_konsisten", "consistency",
             "andal <> (confidence <> 'low')",
             "Flag andal = confidence bukan low"),
        Rule("tahun_bulan_konsisten", "consistency",
             "tahun <> year(acq_date) OR bulan <> month(acq_date)",
             "Kolom tahun/bulan sesuai acq_date"),
    ]
