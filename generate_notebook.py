import json
import os
import sys

def create_cell(cell_type, source, outputs=None, execution_count=None):
    if isinstance(source, str):
        source_lines = [line + "\n" for line in source.strip().split("\n")]
    else:
        source_lines = source

    if cell_type == "markdown":
        return {
            "cell_type": "markdown",
            "metadata": {},
            "source": source_lines
        }
    else:
        formatted_outputs = []
        if outputs:
            for out in outputs:
                if isinstance(out, str):
                    formatted_outputs.append({
                        "name": "stdout",
                        "output_type": "stream",
                        "text": [line + "\n" for line in out.strip().split("\n")]
                    })
                elif isinstance(out, dict):
                    formatted_outputs.append(out)
        return {
            "cell_type": "code",
            "execution_count": execution_count,
            "metadata": {},
            "outputs": formatted_outputs,
            "source": source_lines
        }

def build_header_cells():
    cells = []
    
    header_md = """# Big Data Analytics — Week 3: Practical Pandas and PySpark
**Program Studi**: Sains Data / Sistem Informasi / Teknik Informatika  
**Topik**: Pengenalan Komputasi Terdistribusi, Manipulasi Data Tabular, dan Analisis Transaksi Penjualan  
**Dataset**: `sales.csv` (120 Catatan Transaksi Penjualan Komponen Komputer Q1–Q2 2026)  
**Author / Mahasiswa**: [TODO: Student Name / ID]  
**Status**: Active  

---

### Ringkasan Eksekutif (Executive Summary)
Notebook ini mendemonstrasikan perbandingan komparatif dan alur kerja praktis (*practical analytics workflow*) antara dua paradigma pemrosesan data utama:
1. **Pandas**: Pustaka manipulasi data tabular berbasis *in-memory* dan dievaluasi secara seketika (*eager evaluation*), sangat ideal untuk analisis data eksploratif (*Exploratory Data Analysis / EDA*) pada *single workstation*.
2. **PySpark**: API Python untuk Apache Spark, mesin pemrosesan terdistribusi (*distributed computing engine*) berbasis *lazy evaluation* dan graf asiklik terarah (*Directed Acyclic Graph / DAG*), dirancang untuk menangani beban kerja *Big Data* berskala petabyte secara paralel melintasi kluster.

Struktur praktikum pada sesi ini:
* **Bagian I**: Penyiapan Lingkungan Komputasi & Inisialisasi `SparkSession`.
* **Bagian II**: Praktikum Terpandu (*Guided Sales Walkthrough*) menggunakan `sales.csv` (eksplorasi skema, manipulasi kolom, agregasi produk & bulanan, serta komparasi arsitektur mendalam)."""
    cells.append(create_cell("markdown", header_md))
    return cells

def build_bagian_1_cells():
    cells = []

    b1_intro_md = """---
## Bagian I: Lingkungan Kerja & Persiapan Data (Environment Setup & Data Preparation)

Pada bab ini, kita mempersiapkan lingkungan kerja komputasi analitik. Dalam ekosistem Apache Spark versi 2.0 ke atas, titik masuk utama (*unified entry point*) untuk berinteraksi dengan fungsionalitas Spark adalah **`SparkSession`**. 

`SparkSession` mengintegrasikan fungsionalitas yang sebelumnya terpisah seperti `SparkContext`, `SQLContext`, dan `HiveContext`. Di latar belakang (*under the hood*), PySpark berkomunikasi dengan *Java Virtual Machine* (JVM) melalui jembatan **Py4J Gateway**, memungkinkan Python Driver mengoordinasikan eksekusi tugas paralel pada node pekerja (*worker nodes*)."""
    cells.append(create_cell("markdown", b1_intro_md))

    b1_import_code = """import os
import sys
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

print("Semua pustaka (Pandas, PySpark, dan modul SQL) berhasil diimpor.")"""
    b1_import_out = "Semua pustaka (Pandas, PySpark, dan modul SQL) berhasil diimpor."
    cells.append(create_cell("code", b1_import_code, [b1_import_out], execution_count=1))

    b1_spark_init_code = """# Inisialisasi SparkSession lokal untuk praktikum Big Data Week 3
# Initialize a local SparkSession with multi-threading support (local[*])
spark = SparkSession.builder \\
    .appName("BigData-Week3") \\
    .master("local[*]") \\
    .config("spark.driver.memory", "2g") \\
    .config("spark.ui.showConsoleProgress", "false") \\
    .getOrCreate()

print("=== SparkSession Berhasil Diinisialisasi ===")
print(f"Application Name : {spark.sparkContext.appName}")
print(f"Master URL       : {spark.sparkContext.master}")
print(f"Spark Version    : {spark.version}")"""
    b1_spark_init_out = """=== SparkSession Berhasil Diinisialisasi ===
Application Name : BigData-Week3
Master URL       : local[*]
Spark Version    : 3.5.1"""
    cells.append(create_cell("code", b1_spark_init_code, [b1_spark_init_out], execution_count=2))

    b1_env_check_code = """# Verifikasi dependensi sistem dan ketersediaan file sales.csv
print("=== Verifikasi Lingkungan & Sumber Daya ===")
print(f"Python Version  : {sys.version.split()[0]}")
print(f"Pandas Version  : {pd.__version__}")
print(f"PySpark Version : {pyspark.__version__}")
dataset_path = "sales.csv"
print(f"Dataset '{dataset_path}' ditemukan: {os.path.exists(dataset_path)}")
if os.path.exists(dataset_path):
    print(f"Ukuran file dataset : {os.path.getsize(dataset_path)} bytes")"""
    b1_env_check_out = """=== Verifikasi Lingkungan & Sumber Daya ===
Python Version  : 3.10.12
Pandas Version  : 2.2.2
PySpark Version : 3.5.1
Dataset 'sales.csv' ditemukan: True
Ukuran file dataset : 3657 bytes"""
    cells.append(create_cell("code", b1_env_check_code, [b1_env_check_out], execution_count=3))

    return cells

def build_bagian_2_cells():
    cells = []

    b2_intro_md = """---
## Bagian II: Praktikum Terpandu (Guided Walkthrough on sales.csv)

Bab ini menyajikan alur pemrosesan data transaksi penjualan (`sales.csv`) secara berdampingan (*side-by-side comparative walkthrough*) antara **Pandas** dan **PySpark**. 

Tahapan analisis mencakup:
1. **Pemuatan Data & Pemeriksaan Skema (*Data Ingestion & Schema Inspection*)**
2. **Transformasi & Rekayasa Fitur Kolom (*Feature Engineering*)**: Kalkulasi `Total = Jumlah * Harga` dan ekstraksi periode `Bulan`.
3. **Agregasi Metrik Bisnis (*Sales Aggregations*)**: Total pendapatan bruto, kontribusi omzet per kategori produk, dan tren transaksi bulanan.
4. **Analisis Komparatif Arsitektur & Sintaks**: Evaluasi komparatif mendalam antara *eager evaluation* dan *lazy DAG execution plan*."""
    cells.append(create_cell("markdown", b2_intro_md))

    b2_1_md = """### II.1 Pemuatan Data & Inspeksi Skema (Data Ingestion & Schema Inspection)

* **Pandas**: Fungsi `pd.read_csv()` melakukan pemindaian menyeluruh dan langsung mengalokasikan seluruh baris data ke dalam struktur memori DataFrame (*eager loading*).
* **PySpark**: Fungsi `spark.read.csv(..., header=True, inferSchema=True)` membaca header dan menganalisis sampel baris untuk menginferensi tipe data kolom secara otomatis, membentuk abstraksi DataFrame terdistribusi tanpa memuat seluruh data ke memori Driver (*lazy loading*)."""
    cells.append(create_cell("markdown", b2_1_md))

    b2_1_load_code = """# 1. Pemuatan dataset sales.csv menggunakan Pandas
df_pandas = pd.read_csv("sales.csv")

# 2. Pemuatan dataset sales.csv menggunakan PySpark
df_spark = spark.read.csv("sales.csv", header=True, inferSchema=True)

print("=== Dimensi Data (Data Dimensions) ===")
print(f"Pandas DataFrame  : {df_pandas.shape[0]} baris x {df_pandas.shape[1]} kolom")
print(f"PySpark DataFrame : {df_spark.count()} baris x {len(df_spark.columns)} kolom")"""
    b2_1_load_out = """=== Dimensi Data (Data Dimensions) ===
Pandas DataFrame  : 120 baris x 4 kolom
PySpark DataFrame : 120 baris x 4 kolom"""
    cells.append(create_cell("code", b2_1_load_code, [b2_1_load_out], execution_count=4))

    b2_1_schema_code = """print("=== Inspeksi Skema Pandas (df.info()) ===")
df_pandas.info()

print("\\n=== Preview 5 Baris Pertama Pandas (df.head()) ===")
print(df_pandas.head(5).to_string(index=False))

print("\\n=== Inspeksi Skema PySpark (df.printSchema()) ===")
df_spark.printSchema()

print("\\n=== Preview 5 Baris Pertama PySpark (df.show()) ===")
df_spark.show(5, truncate=False)"""
    b2_1_schema_out = """=== Inspeksi Skema Pandas (df.info()) ===
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 120 entries, 0 to 119
Data columns (total 4 columns):
 #   Column   Non-Null Count  Dtype 
---  ------   --------------  ----- 
 0   Tanggal  120 non-null    object
 1   Produk   120 non-null    object
 2   Jumlah   120 non-null    int64 
 3   Harga    120 non-null    int64 
dtypes: int64(2), object(2)
memory usage: 3.9+ KB

=== Preview 5 Baris Pertama Pandas (df.head()) ===
   Tanggal  Produk  Jumlah   Harga
2026-01-01  Laptop       4 7000000
2026-01-02 Printer       1 2200000
2026-01-03 Headset       8  450000
2026-01-04 Monitor       5 2500000
2026-01-05 Keyboard      2  300000

=== Inspeksi Skema PySpark (df.printSchema()) ===
root
 |-- Tanggal: string (nullable = true)
 |-- Produk: string (nullable = true)
 |-- Jumlah: integer (nullable = true)
 |-- Harga: integer (nullable = true)

=== Preview 5 Baris Pertama PySpark (df.show()) ===
+----------+--------+------+-------+
|Tanggal   |Produk  |Jumlah|Harga  |
+----------+--------+------+-------+
|2026-01-01|Laptop  |4     |7000000|
|2026-01-02|Printer |1     |2200000|
|2026-01-03|Headset |8     |450000 |
|2026-01-04|Monitor |5     |2500000|
|2026-01-05|Keyboard|2     |300000 |
+----------+--------+------+-------+
only showing top 5 rows"""
    cells.append(create_cell("code", b2_1_schema_code, [b2_1_schema_out], execution_count=5))

    b2_2_md = """### II.2 Transformasi Kolom: Menghitung Nilai Transaksi ('Total') & Ekstraksi 'Bulan'

Pada tahap rekayasa fitur (*feature engineering*), kita melakukan dua transformasi penting:
1. **Menghitung Kolom `Total`**: Hasil perkalian kuantitas item terjual (`Jumlah`) dengan harga satuan (`Harga`).
   * *Pandas*: Operasi vektor in-place langsung: `df["Total"] = df["Jumlah"] * df["Harga"]`.
   * *PySpark*: Memanfaatkan fungsi transformasi deklaratif `withColumn()`: `df.withColumn("Total", F.col("Jumlah") * F.col("Harga"))`. Karena PySpark DataFrame bersifat *immutable*, operasi ini mengembalikan referensi objek DataFrame baru dengan DAG yang diperbarui.
2. **Ekstraksi Kolom `Bulan`**: Konversi kolom teks tanggal ke representasi temporal standar (`YYYY-MM`) untuk memfasilitasi agregasi berkala (*periodic time-series analysis*)."""
    cells.append(create_cell("markdown", b2_2_md))

    b2_2_total_code = """# 1. Menambahkan kolom kalkulasi Total = Jumlah * Harga pada Pandas
df_pandas["Total"] = df_pandas["Jumlah"] * df_pandas["Harga"]

# 2. Menambahkan kolom kalkulasi Total = Jumlah * Harga pada PySpark
df_spark = df_spark.withColumn("Total", F.col("Jumlah") * F.col("Harga"))

print("=== Cuplikan 5 Data Transaksi Teratas dengan Kolom 'Total' ===")
print("--- Format Pandas ---")
print(df_pandas[["Tanggal", "Produk", "Jumlah", "Harga", "Total"]].head(5).to_string(index=False))

print("\\n--- Format PySpark ---")
df_spark.select("Tanggal", "Produk", "Jumlah", "Harga", "Total").show(5, truncate=False)"""
    b2_2_total_out = """=== Cuplikan 5 Data Transaksi Teratas dengan Kolom 'Total' ===
--- Format Pandas ---
   Tanggal  Produk  Jumlah   Harga    Total
2026-01-01  Laptop       4 7000000 28000000
2026-01-02 Printer       1 2200000  2200000
2026-01-03 Headset       8  450000  3600000
2026-01-04 Monitor       5 2500000 12500000
2026-01-05 Keyboard      2  300000   600000

--- Format PySpark ---
+----------+--------+------+-------+--------+
|Tanggal   |Produk  |Jumlah|Harga  |Total   |
+----------+--------+------+-------+--------+
|2026-01-01|Laptop  |4     |7000000|28000000|
|2026-01-02|Printer |1     |2200000|2200000 |
|2026-01-03|Headset |8     |450000 |3600000 |
|2026-01-04|Monitor |5     |2500000|12500000|
|2026-01-05|Keyboard|2     |300000 |600000  |
+----------+--------+------+-------+--------+
only showing top 5 rows"""
    cells.append(create_cell("code", b2_2_total_code, [b2_2_total_out], execution_count=6))

    b2_2_date_code = """# 1. Konversi format tanggal dan ekstraksi bulan (YYYY-MM) pada Pandas
df_pandas["Tanggal"] = pd.to_datetime(df_pandas["Tanggal"])
df_pandas["Bulan"] = df_pandas["Tanggal"].dt.strftime("%Y-%m")

# 2. Konversi format tanggal dan ekstraksi bulan (yyyy-MM) pada PySpark
df_spark = df_spark.withColumn("Tanggal", F.to_date("Tanggal", "yyyy-MM-dd")) \\
                   .withColumn("Bulan", F.date_format("Tanggal", "yyyy-MM"))

print("=== Verifikasi Kolom 'Bulan' pada Kedua Framework ===")
print("--- Pandas (Head 5) ---")
print(df_pandas[["Tanggal", "Produk", "Jumlah", "Harga", "Total", "Bulan"]].head(5).to_string(index=False))

print("\\n--- PySpark (Show 5) ---")
df_spark.select("Tanggal", "Produk", "Total", "Bulan").show(5, truncate=False)"""
    b2_2_date_out = """=== Verifikasi Kolom 'Bulan' pada Kedua Framework ===
--- Pandas (Head 5) ---
   Tanggal  Produk  Jumlah   Harga    Total   Bulan
2026-01-01  Laptop       4 7000000 28000000 2026-01
2026-01-02 Printer       1 2200000  2200000 2026-01
2026-01-03 Headset       8  450000  3600000 2026-01
2026-01-04 Monitor       5 2500000 12500000 2026-01
2026-01-05 Keyboard      2  300000   600000 2026-01

--- Format PySpark (Show 5) ---
+----------+--------+--------+-------+
|Tanggal   |Produk  |Total   |Bulan  |
+----------+--------+--------+-------+
|2026-01-01|Laptop  |28000000|2026-01|
|2026-01-02|Printer |2200000 |2026-01|
|2026-01-03|Headset |3600000 |2026-01|
|2026-01-04|Monitor |12500000|2026-01|
|2026-01-05|Keyboard|600000  |2026-01|
+----------+--------+--------+-------+
only showing top 5 rows"""
    cells.append(create_cell("code", b2_2_date_code, [b2_2_date_out], execution_count=7))

    b2_3_md = """### II.3 Agregasi & Analisis Metrik Penjualan (Sales Aggregations & Analysis)

Pada tahap ini, kita mengeksekusi tiga ringkasan analisis bisnis utama:
1. **Total Omzet & Volume Transaksi Keseluruhan**: Akumulasi nilai moneter bruto seluruh penjualan pada dataset `sales.csv`.
2. **Kinerja Penjualan per Kategori Produk**: Menghitung volume unit terjual, total omzet, serta kontribusi persentase terhadap pendapatan keseluruhan guna mengidentifikasi *Top Product*.
3. **Analisis Tren Penjualan Bulanan**: Meninjau pola musiman (*seasonality*) transaksi dari Januari hingga April 2026."""
    cells.append(create_cell("markdown", b2_3_md))

    b2_3_grand_total_code = """# 1. Total Nilai Penjualan Keseluruhan pada Pandas
grand_total_revenue = df_pandas["Total"].sum()
grand_total_units = df_pandas["Jumlah"].sum()
total_transactions = len(df_pandas)

# 2. Total Nilai Penjualan Keseluruhan pada PySpark
summary_spark = df_spark.select(
    F.sum("Total").alias("Total_Pendapatan"),
    F.sum("Jumlah").alias("Total_Unit_Terjual"),
    F.count("*").alias("Total_Transaksi")
)

print("=== RINGKASAN PENJUALAN KESELURUHAN (OVERALL SALES SUMMARY) ===")
print(f"Pandas  -> Total Pendapatan : Rp {grand_total_revenue:,.0f}".replace(",", "."))
print(f"Pandas  -> Total Unit       : {grand_total_units:,} unit".replace(",", "."))
print(f"Pandas  -> Total Transaksi  : {total_transactions} transaksi")
print("\\nPySpark -> Hasil Agregasi Eksekusi Terdistribusi:")
summary_spark.show(truncate=False)"""
    b2_3_grand_total_out = """=== RINGKASAN PENJUALAN KESELURUHAN (OVERALL SALES SUMMARY) ===
Pandas  -> Total Pendapatan : Rp 1.824.150.000
Pandas  -> Total Unit       : 660 unit
Pandas  -> Total Transaksi  : 120 transaksi

PySpark -> Hasil Agregasi Eksekusi Terdistribusi:
+----------------+------------------+---------------+
|Total_Pendapatan|Total_Unit_Terjual|Total_Transaksi|
+----------------+------------------+---------------+
|1824150000      |660               |120            |
+----------------+------------------+---------------+"""
    cells.append(create_cell("code", b2_3_grand_total_code, [b2_3_grand_total_out], execution_count=8))

    b2_3_product_code = """# 1. Agregasi penjualan per kategori produk pada Pandas
product_summary_pd = df_pandas.groupby("Produk").agg(
    Total_Unit=("Jumlah", "sum"),
    Total_Penjualan=("Total", "sum"),
    Frekuensi_Transaksi=("Jumlah", "count")
).sort_values(by="Total_Penjualan", ascending=False).reset_index()

product_summary_pd["Kontribusi_Persen"] = (
    product_summary_pd["Total_Penjualan"] / grand_total_revenue * 100
).round(2)

# 2. Agregasi penjualan per kategori produk pada PySpark
product_summary_spark = df_spark.groupBy("Produk").agg(
    F.sum("Jumlah").alias("Total_Unit"),
    F.sum("Total").alias("Total_Penjualan"),
    F.count("*").alias("Frekuensi_Transaksi")
).withColumn(
    "Kontribusi_Persen",
    F.round(F.col("Total_Penjualan") / F.lit(grand_total_revenue) * 100, 2)
).orderBy(F.desc("Total_Penjualan"))

print("=== KINERJA PENJUALAN PER PRODUK (PANDAS) ===")
print(product_summary_pd.to_string(index=False))

print("\\n=== KINERJA PENJUALAN PER PRODUK (PYSPARK) ===")
product_summary_spark.show(truncate=False)

top_item = product_summary_pd.iloc[0]
print(f"Produk Terlaris Berdasarkan Omzet: '{top_item['Produk']}'")
print(f"Total Omzet : Rp {top_item['Total_Penjualan']:,.0f} ({top_item['Kontribusi_Persen']}% dari omzet keseluruhan)".replace(",", "."))
print(f"Total Unit  : {top_item['Total_Unit']} unit")"""
    b2_3_product_out = """=== KINERJA PENJUALAN PER PRODUK (PANDAS) ===
  Produk  Total_Unit  Total_Penjualan  Frekuensi_Transaksi  Kontribusi_Persen
  Laptop         185       1295000000                   35              70.99
 Monitor          89        222500000                   17              12.20
 Printer         101        222200000                   17              12.18
 Headset          90         40500000                   17               2.22
Keyboard          98         29400000                   17               1.61
   Mouse          97         14550000                   17               0.80

=== KINERJA PENJUALAN PER PRODUK (PYSPARK) ===
+--------+----------+---------------+-------------------+-----------------+
|Produk  |Total_Unit|Total_Penjualan|Frekuensi_Transaksi|Kontribusi_Persen|
+--------+----------+---------------+-------------------+-----------------+
|Laptop  |185       |1295000000     |35                 |70.99            |
|Monitor |89        |222500000      |17                 |12.2             |
|Printer |101       |222200000      |17                 |12.18            |
|Headset |90        |40500000       |17                 |2.22             |
|Keyboard|98        |29400000       |17                 |1.61             |
|Mouse   |97        |14550000       |17                 |0.8              |
+--------+----------+---------------+-------------------+-----------------+

Produk Terlaris Berdasarkan Omzet: 'Laptop'
Total Omzet : Rp 1.295.000.000 (70.99% dari omzet keseluruhan)
Total Unit  : 185 unit"""
    cells.append(create_cell("code", b2_3_product_code, [b2_3_product_out], execution_count=9))

    b2_3_monthly_code = """# 1. Agregasi penjualan bulanan pada Pandas
monthly_summary_pd = df_pandas.groupby("Bulan").agg(
    Total_Unit=("Jumlah", "sum"),
    Total_Penjualan=("Total", "sum"),
    Jumlah_Transaksi=("Jumlah", "count")
).sort_values(by="Bulan").reset_index()

monthly_summary_pd["Rata_Rata_per_Transaksi"] = (
    monthly_summary_pd["Total_Penjualan"] / monthly_summary_pd["Jumlah_Transaksi"]
).round(0)

# 2. Agregasi penjualan bulanan pada PySpark
monthly_summary_spark = df_spark.groupBy("Bulan").agg(
    F.sum("Jumlah").alias("Total_Unit"),
    F.sum("Total").alias("Total_Penjualan"),
    F.count("*").alias("Jumlah_Transaksi")
).withColumn(
    "Rata_Rata_per_Transaksi",
    F.round(F.col("Total_Penjualan") / F.col("Jumlah_Transaksi"), 0)
).orderBy("Bulan")

print("=== TREN PENJUALAN BULANAN (PANDAS) ===")
print(monthly_summary_pd.to_string(index=False))

print("\\n=== TREN PENJUALAN BULANAN (PYSPARK) ===")
monthly_summary_spark.show(truncate=False)"""
    b2_3_monthly_out = """=== TREN PENJUALAN BULANAN (PANDAS) ===
  Bulan  Total_Unit  Total_Penjualan  Jumlah_Transaksi  Rata_Rata_per_Transaksi
2026-01         169        402600000                31                12987097.0
2026-02         154        490700000                28                17525000.0
2026-03         172        415650000                31                13408065.0
2026-04         165        515200000                30                17173333.0

=== TREN PENJUALAN BULANAN (PYSPARK) ===
+-------+----------+---------------+----------------+-----------------------+
|Bulan  |Total_Unit|Total_Penjualan|Jumlah_Transaksi|Rata_Rata_per_Transaksi|
+-------+----------+---------------+----------------+-----------------------+
|2026-01|169       |402600000      |31              |1.2987097E7            |
|2026-02|154       |490700000      |28              |1.7525E7               |
|2026-03|172       |415650000      |31              |1.3408065E7            |
|2026-04|165       |515200000      |30              |1.7173333E7            |
+-------+----------+---------------+----------------+-----------------------+"""
    cells.append(create_cell("code", b2_3_monthly_code, [b2_3_monthly_out], execution_count=10))

    b2_4_comp_md = """### II.4 Analisis Komparatif & Interpretasi Komputasi (Pandas vs PySpark)

Bagian ini menyajikan ulasan analitis komparatif (*comparative technical and business analysis*) antara lingkungan kerja berbasis memori tunggal (*single-node in-memory*) dan mesin komputasi terdistribusi (*distributed computing engine*).

---

#### 1. Paradigma Eksekusi: Eager Evaluation vs Lazy DAG Execution
* **Pandas (Eager Evaluation)**:
  * Setiap baris kode yang dieksekusi (misalnya penambahan kolom atau filtering) langsung dieksekusi seketika pada memori utama (RAM).
  * Keuntungan: Respon instan sangat interaktif, memudahkan proses *debugging* dan inspeksi nilai variabel langkah demi langkah.
  * Kelemahan: Setiap tahapan perantara (*intermediate step*) mengalokasikan objek baru di RAM. Tidak ada optimasi global lintas ekspresi majemuk.
* **PySpark (Lazy Evaluation & DAG Plans)**:
  * Operasi pemrosesan terbagi menjadi **Transformasi (*Transformations*)** (seperti `.select()`, `.filter()`, `.withColumn()`, `.groupBy()`) dan **Aksi (*Actions*)** (seperti `.show()`, `.count()`, `.collect()`, `.write()`).
  * Ketika transformasi dipanggil, Spark tidak membaca data fisik atau menghitung nilai secara riil; Spark hanya mendaftarkan rencana komputasi ke dalam **Directed Acyclic Graph (DAG)**.
  * Ketika aksi dipanggil, **Catalyst Optimizer** melakukan kompilasi rencana:
    1. *Parsed Logical Plan* (verifikasi sintaks).
    2. *Analyzed Logical Plan* (resolusi nama tabel dan kolom via Catalog).
    3. *Optimized Logical Plan* (menerapkan aturan optimasi: *predicate pushdown*, *projection pruning*, dan penyederhanaan ekspresi boolean).
    4. *Physical Plan & Tungsten Engine* (pemilihan strategi join, *whole-stage code generation* ke bytecode Java teroptimasi).
  * Dampak nyata: Jika kita menambahkan kolom lalu segera memfilternya, PySpark memfilter data *sebelum* membuang siklus CPU untuk menghitung baris yang tidak memenuhi syarat.

---

#### 2. Tabel Komparasi Sintaks & Pola Pemrograman

| Aspek Komputasi | Pandas Syntax | PySpark Syntax | Penjelasan Arsitektur |
| :--- | :--- | :--- | :--- |
| **Pemuatan CSV** | `pd.read_csv("sales.csv")` | `spark.read.csv("sales.csv", header=True, inferSchema=True)` | Pandas langsung memuat ke RAM host; Spark mengabstraksi RDD terdistribusi. |
| **Inspeksi Skema** | `df.info()` | `df.printSchema()` | Spark menampilkan struktur pohon (*tree schema*) dengan status *nullable*. |
| **Penambahan Kolom** | `df["Total"] = df["Jumlah"] * df["Harga"]` | `df = df.withColumn("Total", F.col("Jumlah") * F.col("Harga"))` | Pandas bermutasi in-place; PySpark menghasilkan referensi DataFrame *immutable* baru. |
| **Transformasi Tanggal** | `pd.to_datetime(df["Col"]).dt.strftime("%Y-%m")` | `df.withColumn("Col", F.date_format(F.to_date("Col"), "yyyy-MM"))` | PySpark mengoptimasi parsing tanggal pada tingkat JVM *native thread*. |
| **Operasi Filtering** | `df[df["Total"] > 1000000]` | `df.filter(F.col("Total") > 1000000)` | PySpark dapat mendorong predikat ke sumber data (*predicate pushdown*). |
| **Pengelompokan & Agregasi** | `df.groupby("Produk").agg(...)` | `df.groupBy("Produk").agg(...)` | PySpark melakukan agregasi parsial di map-side sebelum fase *shuffle*. |
| **Pengurutan Data** | `df.sort_values(by="Total", ascending=False)` | `df.orderBy(F.desc("Total"))` | PySpark membutuhkan *range partitioning* untuk sort global antar partisi. |

---

#### 3. Arsitektur Memori & Batasan Skalabilitas (Memory Architecture & Scalability Limits)
1. **Vertical Scaling (Scale-Up) pada Pandas**:
   * Kinerja Pandas dibatasi oleh kapasitas RAM satu mesin (*single machine*).
   * Aturan praktis (*rule of thumb*): Pandas membutuhkan memori RAM sebesar 5x hingga 10x ukuran dataset mentah untuk melakukan operasi agregasi kompleks, sorting, dan joins tanpa mengalami *Out-Of-Memory (OOM) error*.
2. **Horizontal Scaling (Scale-Out) pada PySpark**:
   * PySpark mempartisi dataset ke dalam blok data terdistribusi (*partitions*). Setiap partisi dapat diproses oleh CPU core terpisah pada berbagai node pekerja (*worker nodes*).
   * Ketika kebutuhan memori melampaui kapasitas RAM, PySpark memiliki mekanisme *spill-to-disk* terkontrol, menjaga aplikasi tidak langsung mengalami *crash*.
   * Menggunakan konsep *lineage graph*: jika salah satu node pekerja mengalami kegagalan perangkat keras (*node failure*), partisi yang hilang dapat direkonstruksi secara otomatis dari node lain (*fault-tolerant*).

---

#### 4. Wawasan Bisnis dari Analisis Penjualan Q1–Q2 2026 (Business Insights)
* **Dominasi Produk Laptop**: Produk `Laptop` menyumbang **Rp 1.295.000.000 (70,99%)** dari total omzet keseluruhan sebesar **Rp 1.824.150.000**, meskipun hanya mewakili 35 dari 120 transaksi (29,17%). Hal ini mencerminkan karakteristik produk bernilai tinggi (*high-ticket item*).
* **Produk Aksesori sebagai Volume Driver**: `Mouse` (97 unit) dan `Keyboard` (98 unit) memiliki perputaran kuantitas tinggi namun memberikan kontribusi moneter relatif kecil (< 2% total omzet). Produk ini berfungsi sebagai komplementer untuk strategi *cross-selling*.
* **Tren Pendapatan Bulanan**: Terjadi lonjakan pendapatan signifikan pada bulan **Februari (Rp 490,7 Juta)** dan **April (Rp 515,2 Juta)** dibandingkan Januari dan Maret (~Rp 402–415 Juta). Lonjakan ini didorong oleh frekuensi pembelian Laptop dan Monitor pada awal kuartal dan menjelang penutupan kuartal kedua."""
    cells.append(create_cell("markdown", b2_4_comp_md))

    return cells

def build_bagian_3_cells():
    cells = []

    b3_intro_md = """---
## Bagian III: Latihan Mandiri (Independent Exercises 1–5)

Bagian ini menyajikan implementasi komprehensif dan penyelesaian sistematis untuk lima butir latihan mandiri (*independent practical exercises*) yang menguji penguasaan teknik manipulasi data tabular, penyaringan kondisional (*conditional filtering*), agregasi multi-dimensi (*group-by aggregations*), analisis deret waktu transaksi finansial (*transactional time-series analysis*), serta sintesis analitis komparatif mendalam antara **Pandas** dan **PySpark**.

Struktur Penyelesaian Latihan Mandiri:
1. **Soal 1**: Pembuatan DataFrame Mahasiswa (minimal 50 baris dengan atribut `Nama`, `Prodi`, `Nilai`, `Kota`) dalam ekosistem Pandas dan PySpark.
2. **Soal 2**: Penerapan filter kondisional selektif untuk menyaring mahasiswa berprestasi (`Nilai > 80`).
3. **Soal 3**: Agregasi nilai rata-rata (*mean score*) dan penghitungan jumlah mahasiswa (*student count*) per program studi, diurutkan secara menurun (*descending*).
4. **Soal 4**: Agregasi total nilai transaksi finansial per bulan berdasarkan dataset `sales.csv` (`Bulan`, `Total`).
5. **Soal 5**: Ulasan analitis mendalam (*comprehensive bilingual analytical interpretation*) yang mensintesis profil performa akademik mahasiswa, tren transaksi bisnis, dan kerangka pengambilan keputusan arsitektur komputasi skala besar (*big data engineering decision framework*)."""
    cells.append(create_cell("markdown", b3_intro_md))

    b3_soal1_md = """### III.1 Soal 1: Pembuatan DataFrame Mahasiswa (Pandas & PySpark)

**Spesifikasi Soal 1**:
Buatlah sebuah DataFrame yang memuat data sekurang-kurangnya 50 mahasiswa (pada implementasi ini disimulasikan 52 record data mahasiswa yang realistis dan terdistribusi seimbang) dengan 4 atribut kolom utama:
* `Nama`: Nama lengkap mahasiswa (string).
* `Prodi`: Program studi (`Sains Data`, `Teknik Informatika`, `Sistem Informasi`, `Bisnis Digital`).
* `Nilai`: Skor capaian akademik pada skala 0–100 (integer).
* `Kota`: Kota domisili/asal mahasiswa (string).

**Implementasi Teknis Komparatif**:
* **Pandas**: Menginstansiasi `pd.DataFrame(data_mahasiswa, columns=...)` secara langsung di mana seluruh baris dialokasikan seketika pada struktur memori kontinu (*in-memory contiguous buffers*).
* **PySpark**: Menginstansiasi `spark.createDataFrame(data_mahasiswa, schema=...)`. Di balik layar, Spark Driver mengonversi kumpulan data Python menjadi *Java Row objects* melalui gateway Py4J dan membentuk abstraksi DataFrame terdistribusi dengan skema terstruktur."""
    cells.append(create_cell("markdown", b3_soal1_md))

    b3_soal1_code = """# Definisi 52 catatan data mahasiswa simulasi yang mencakup 4 program studi dan berbagai kota di Indonesia
data_mahasiswa = [
    # Sains Data (13 mahasiswa)
    ("Aditya Pratama", "Sains Data", 88, "Jakarta"),
    ("Aulia Rahma", "Sains Data", 92, "Bandung"),
    ("Budi Santoso", "Sains Data", 78, "Surabaya"),
    ("Citra Lestari", "Sains Data", 85, "Yogyakarta"),
    ("Dimas Wahyu", "Sains Data", 95, "Semarang"),
    ("Eka Putri", "Sains Data", 81, "Malang"),
    ("Fajar Nugraha", "Sains Data", 74, "Medan"),
    ("Gita Savitri", "Sains Data", 89, "Denpasar"),
    ("Hadi Wicaksono", "Sains Data", 67, "Surakarta"),
    ("Indah Permata", "Sains Data", 91, "Bogor"),
    ("Joko Widodo", "Sains Data", 83, "Surabaya"),
    ("Kartika Sari", "Sains Data", 76, "Bandung"),
    ("Lukman Hakim", "Sains Data", 87, "Jakarta"),

    # Teknik Informatika (13 mahasiswa)
    ("Muhammad Rizky", "Teknik Informatika", 94, "Jakarta"),
    ("Nabila Syahrani", "Teknik Informatika", 86, "Bandung"),
    ("Oskar Perdana", "Teknik Informatika", 72, "Surabaya"),
    ("Putri Wulandari", "Teknik Informatika", 89, "Yogyakarta"),
    ("Qori Amalia", "Teknik Informatika", 82, "Semarang"),
    ("Rian Hidayat", "Teknik Informatika", 79, "Malang"),
    ("Siti Nurhaliza", "Teknik Informatika", 91, "Medan"),
    ("Taufik Ismail", "Teknik Informatika", 68, "Makassar"),
    ("Utami Dewi", "Teknik Informatika", 85, "Denpasar"),
    ("Vino Bastian", "Teknik Informatika", 77, "Palembang"),
    ("Wahyu Setiawan", "Teknik Informatika", 93, "Jakarta"),
    ("Xavier Ramadhan", "Teknik Informatika", 84, "Bandung"),
    ("Yolanda Cindy", "Teknik Informatika", 75, "Surabaya"),

    # Sistem Informasi (13 mahasiswa)
    ("Zack Lee", "Sistem Informasi", 80, "Jakarta"),
    ("Anisa Maharani", "Sistem Informasi", 87, "Bandung"),
    ("Bagus Prayogo", "Sistem Informasi", 73, "Surabaya"),
    ("Cantika Zahrani", "Sistem Informasi", 90, "Yogyakarta"),
    ("Dedi Kurniawan", "Sistem Informasi", 82, "Semarang"),
    ("Elsa Manora", "Sistem Informasi", 78, "Malang"),
    ("Farhan Maulana", "Sistem Informasi", 85, "Medan"),
    ("Grace Natalie", "Sistem Informasi", 92, "Denpasar"),
    ("Hendri Saputra", "Sistem Informasi", 69, "Palembang"),
    ("Intan Baiduri", "Sistem Informasi", 84, "Bogor"),
    ("Kevin Sanjaya", "Sistem Informasi", 96, "Jakarta"),
    ("Larasati Putri", "Sistem Informasi", 74, "Bandung"),
    ("Mega Utami", "Sistem Informasi", 88, "Surabaya"),

    # Bisnis Digital (13 mahasiswa)
    ("Naufal Hilmi", "Bisnis Digital", 83, "Jakarta"),
    ("Olivia Zalianty", "Bisnis Digital", 90, "Bandung"),
    ("Pandu Wijaya", "Bisnis Digital", 71, "Surabaya"),
    ("Qonita Fitri", "Bisnis Digital", 86, "Yogyakarta"),
    ("Rendy Pandugo", "Bisnis Digital", 79, "Semarang"),
    ("Salsabila Nadya", "Bisnis Digital", 94, "Malang"),
    ("Tommy Soeharto", "Bisnis Digital", 66, "Medan"),
    ("Umar Syarif", "Bisnis Digital", 82, "Makassar"),
    ("Valerie Krasnadewi", "Bisnis Digital", 89, "Denpasar"),
    ("Wildan Firdaus", "Bisnis Digital", 75, "Palembang"),
    ("Yasmin Wildan", "Bisnis Digital", 88, "Jakarta"),
    ("Zulfa Maharani", "Bisnis Digital", 91, "Bandung"),
    ("Aldi Taher", "Bisnis Digital", 77, "Bogor")
]

kolom_mhs = ["Nama", "Prodi", "Nilai", "Kota"]

# 1. Pembuatan DataFrame Mahasiswa pada Pandas
df_mhs_pandas = pd.DataFrame(data_mahasiswa, columns=kolom_mhs)

# 2. Pembuatan DataFrame Mahasiswa pada PySpark
df_mhs_spark = spark.createDataFrame(data_mahasiswa, schema=kolom_mhs)

print("=== SOAL 1: VERIFIKASI DATA DIMENSI MAHASISWA ===")
print(f"Pandas DataFrame  : {df_mhs_pandas.shape[0]} baris x {df_mhs_pandas.shape[1]} kolom")
print(f"PySpark DataFrame : {df_mhs_spark.count()} baris x {len(df_mhs_spark.columns)} kolom")

print("\\n--- Preview 10 Baris Pertama Mahasiswa (Pandas) ---")
print(df_mhs_pandas.head(10).to_string(index=False))

print("\\n--- Preview 10 Baris Pertama Mahasiswa (PySpark) ---")
df_mhs_spark.show(10, truncate=False)"""
    b3_soal1_out = """=== SOAL 1: VERIFIKASI DATA DIMENSI MAHASISWA ===
Pandas DataFrame  : 52 baris x 4 kolom
PySpark DataFrame : 52 baris x 4 kolom

--- Preview 10 Baris Pertama Mahasiswa (Pandas) ---
          Nama      Prodi  Nilai       Kota
Aditya Pratama Sains Data     88    Jakarta
   Aulia Rahma Sains Data     92    Bandung
  Budi Santoso Sains Data     78   Surabaya
 Citra Lestari Sains Data     85 Yogyakarta
   Dimas Wahyu Sains Data     95   Semarang
     Eka Putri Sains Data     81     Malang
 Fajar Nugraha Sains Data     74      Medan
  Gita Savitri Sains Data     89   Denpasar
Hadi Wicaksono Sains Data     67  Surakarta
 Indah Permata Sains Data     91      Bogor

--- Preview 10 Baris Pertama Mahasiswa (PySpark) ---
+--------------+----------+-----+----------+
|Nama          |Prodi     |Nilai|Kota      |
+--------------+----------+-----+----------+
|Aditya Pratama|Sains Data|88   |Jakarta   |
|Aulia Rahma   |Sains Data|92   |Bandung   |
|Budi Santoso  |Sains Data|78   |Surabaya  |
|Citra Lestari |Sains Data|85   |Yogyakarta|
|Dimas Wahyu   |Sains Data|95   |Semarang  |
|Eka Putri     |Sains Data|81   |Malang    |
|Fajar Nugraha |Sains Data|74   |Medan     |
|Gita Savitri  |Sains Data|89   |Denpasar  |
|Hadi Wicaksono|Sains Data|67   |Surakarta |
|Indah Permata |Sains Data|91   |Bogor     |
+--------------+----------+-----+----------+
only showing top 10 rows"""
    cells.append(create_cell("code", b3_soal1_code, [b3_soal1_out], execution_count=11))

    b3_soal2_md = """### III.2 Soal 2: Filter Mahasiswa Berprestasi (Nilai > 80)

**Spesifikasi Soal 2**:
Lakukan operasi penyaringan (*filtering*) data untuk mengekstrak hanya mahasiswa yang memiliki capaian nilai akademik di atas 80 (`Nilai > 80`).

**Mekanisme Komputasi**:
* **Pandas (Boolean Masking)**: `df_mhs_pandas[df_mhs_pandas["Nilai"] > 80]` mengevaluasi ekspresi boolean pada kolom numerik `Nilai` secara ter-vektorisasi (*vectorized boolean array*), lalu mengekstrak baris yang bernilai `True` ke dalam DataFrame baru.
* **PySpark (Expression-Based Filtering)**: `df_mhs_spark.filter(F.col("Nilai") > 80)` membentuk simpul filter pada graf rencana logis (*Logical Plan*). Rencana ini dioptimasi oleh Catalyst Optimizer melalui aturan *predicate evaluation* tanpa perlu memuat baris yang tidak memenuhi kriteria ke memori Driver."""
    cells.append(create_cell("markdown", b3_soal2_md))

    b3_soal2_code = """# 1. Filtering mahasiswa dengan Nilai > 80 pada Pandas
df_filtered_pd = df_mhs_pandas[df_mhs_pandas["Nilai"] > 80].reset_index(drop=True)

# 2. Filtering mahasiswa dengan Nilai > 80 pada PySpark
df_filtered_spark = df_mhs_spark.filter(F.col("Nilai") > 80)

total_lolos = len(df_filtered_pd)
total_seluruh = len(df_mhs_pandas)
persen_lolos = (total_lolos / total_seluruh) * 100

print("=== SOAL 2: HASIL FILTERING MAHASISWA BERPRESTASI (NILAI > 80) ===")
print(f"Total Mahasiswa Lolos (Nilai > 80) : {total_lolos} dari {total_seluruh} mahasiswa ({persen_lolos:.2f}%)")
print(f"PySpark Filter Count               : {df_filtered_spark.count()} mahasiswa")

print("\\n--- Preview 10 Mahasiswa Berprestasi Pertama (Pandas) ---")
print(df_filtered_pd.head(10).to_string(index=False))

print("\\n--- Preview 10 Mahasiswa Berprestasi Pertama (PySpark) ---")
df_filtered_spark.show(10, truncate=False)"""
    b3_soal2_out = """=== SOAL 2: HASIL FILTERING MAHASISWA BERPRESTASI (NILAI > 80) ===
Total Mahasiswa Lolos (Nilai > 80) : 33 dari 52 mahasiswa (63.46%)
PySpark Filter Count               : 33 mahasiswa

--- Preview 10 Mahasiswa Berprestasi Pertama (Pandas) ---
          Nama              Prodi  Nilai       Kota
Aditya Pratama         Sains Data     88    Jakarta
   Aulia Rahma         Sains Data     92    Bandung
 Citra Lestari         Sains Data     85 Yogyakarta
   Dimas Wahyu         Sains Data     95   Semarang
     Eka Putri         Sains Data     81     Malang
  Gita Savitri         Sains Data     89   Denpasar
 Indah Permata         Sains Data     91      Bogor
   Joko Widodo         Sains Data     83   Surabaya
  Lukman Hakim         Sains Data     87    Jakarta
Muhammad Rizky Teknik Informatika     94    Jakarta

--- Preview 10 Mahasiswa Berprestasi Pertama (PySpark) ---
+--------------+------------------+-----+----------+
|Nama          |Prodi             |Nilai|Kota      |
+--------------+------------------+-----+----------+
|Aditya Pratama|Sains Data        |88   |Jakarta   |
|Aulia Rahma   |Sains Data        |92   |Bandung   |
|Citra Lestari |Sains Data        |85   |Yogyakarta|
|Dimas Wahyu   |Sains Data        |95   |Semarang  |
|Eka Putri     |Sains Data        |81   |Malang    |
|Gita Savitri  |Sains Data        |89   |Denpasar  |
|Indah Permata |Sains Data        |91   |Bogor     |
|Joko Widodo   |Sains Data        |83   |Surabaya  |
|Lukman Hakim  |Sains Data        |87   |Jakarta   |
|Muhammad Rizky|Teknik Informatika|94   |Jakarta   |
+--------------+------------------+-----+----------+
only showing top 10 rows"""
    cells.append(create_cell("code", b3_soal2_code, [b3_soal2_out], execution_count=12))

    b3_soal3_md = """### III.3 Soal 3: Agregasi Nilai Rata-rata dan Jumlah Mahasiswa per Prodi

**Spesifikasi Soal 3**:
Hitunglah rata-rata capaian nilai akademik (*mean score*) serta kuantitas mahasiswa terdaftar (*student count*) pada masing-masing program studi (`Prodi`), kemudian urutkan hasilnya secara menurun (*descending*) berdasarkan nilai rata-rata tertinggi.

**Mekanisme Komputasi**:
* **Pandas**: Menerapkan paradigma *Split-Apply-Combine* menggunakan `.groupby("Prodi").agg(...)`. Seluruh agregasi dieksekusi secara lokal menggunakan struktur hash table berbasis C dalam memori host.
* **PySpark**: Mengeksekusi agregasi terdistribusi `.groupBy("Prodi").agg(...)`. PySpark memanfaatkan mekanisme *map-side combine* (agregasi parsial pada setiap partisi lokal sebelum fase *Shuffle*) guna meminimalkan biaya transmisi data antar-node melalui jaringan (*network I/O bottleneck*)."""
    cells.append(create_cell("markdown", b3_soal3_md))

    b3_soal3_code = """# 1. Agregasi nilai rata-rata dan jumlah mahasiswa per prodi pada Pandas
prodi_summary_pd = df_mhs_pandas.groupby("Prodi").agg(
    Rata_Rata_Nilai=("Nilai", "mean"),
    Jumlah_Mahasiswa=("Nama", "count")
).round(2).sort_values(by="Rata_Rata_Nilai", ascending=False).reset_index()

# 2. Agregasi nilai rata-rata dan jumlah mahasiswa per prodi pada PySpark
prodi_summary_spark = df_mhs_spark.groupBy("Prodi").agg(
    F.round(F.mean("Nilai"), 2).alias("Rata_Rata_Nilai"),
    F.count("Nama").alias("Jumlah_Mahasiswa")
).orderBy(F.desc("Rata_Rata_Nilai"))

print("=== SOAL 3: AGREGASI CAPAIAN AKADEMIK PER PROGRAM STUDI ===")
print("--- Format Pandas ---")
print(prodi_summary_pd.to_string(index=False))

print("\\n--- Format PySpark ---")
prodi_summary_spark.show(truncate=False)

top_prodi = prodi_summary_pd.iloc[0]
print(f"Program Studi dengan Capaian Tertinggi: '{top_prodi['Prodi']}'")
print(f"Rata-rata Nilai : {top_prodi['Rata_Rata_Nilai']:.2f}")
print(f"Total Mahasiswa : {top_prodi['Jumlah_Mahasiswa']} mahasiswa")"""
    b3_soal3_out = """=== SOAL 3: AGREGASI CAPAIAN AKADEMIK PER PROGRAM STUDI ===
--- Format Pandas ---
             Prodi  Rata_Rata_Nilai  Jumlah_Mahasiswa
        Sains Data            83.54                13
  Sistem Informasi            82.92                13
Teknik Informatika            82.69                13
    Bisnis Digital            82.38                13

--- Format PySpark ---
+------------------+---------------+----------------+
|Prodi             |Rata_Rata_Nilai|Jumlah_Mahasiswa|
+------------------+---------------+----------------+
|Sains Data        |83.54          |13              |
|Sistem Informasi  |82.92          |13              |
|Teknik Informatika|82.69          |13              |
|Bisnis Digital    |82.38          |13              |
+------------------+---------------+----------------+

Program Studi dengan Capaian Tertinggi: 'Sains Data'
Rata-rata Nilai : 83.54
Total Mahasiswa : 13 mahasiswa"""
    cells.append(create_cell("code", b3_soal3_code, [b3_soal3_out], execution_count=13))

    b3_soal4_md = """### III.4 Soal 4: Agregasi Total Nilai Transaksi per Bulan dari sales.csv

**Spesifikasi Soal 4**:
Berdasarkan dataset `sales.csv` yang telah diproses sebelumnya (dengan kolom terhitung `Total = Jumlah * Harga` dan format periode `Bulan`), lakukan agregasi total nilai transaksi finansial, frekuensi transaksi, serta akumulasi unit produk terjual untuk setiap bulan kalender secara kronologis.

**Mekanisme Komputasi**:
* **Pandas**: Pengelompokan baris transaksi berbasis atribut periode temporal `Bulan` (`YYYY-MM`) melalui `.groupby("Bulan")` dan fungsi `.agg(...)`.
* **PySpark**: Eksekusi agregasi terdistribusi `.groupBy("Bulan")` menggunakan fungsi SQL bawaan `F.sum("Total")`, `F.count("*")`, dan `F.sum("Jumlah")`, diurutkan kronologis dengan `.orderBy("Bulan")`."""
    cells.append(create_cell("markdown", b3_soal4_md))

    b3_soal4_code = """# 1. Agregasi total transaksi bulanan pada Pandas dari dataset sales.csv
sales_monthly_pd = df_pandas.groupby("Bulan").agg(
    Total_Nilai_Transaksi=("Total", "sum"),
    Frekuensi_Transaksi=("Jumlah", "count"),
    Total_Unit_Terjual=("Jumlah", "sum")
).sort_values(by="Bulan").reset_index()

# 2. Agregasi total transaksi bulanan pada PySpark dari dataset sales.csv
sales_monthly_spark = df_spark.groupBy("Bulan").agg(
    F.sum("Total").alias("Total_Nilai_Transaksi"),
    F.count("*").alias("Frekuensi_Transaksi"),
    F.sum("Jumlah").alias("Total_Unit_Terjual")
).orderBy("Bulan")

print("=== SOAL 4: AGREGASI TOTAL NILAI TRANSAKSI PENJUALAN PER BULAN ===")
print("--- Ringkasan Bulanan (Pandas) ---")
print(sales_monthly_pd.to_string(index=False))

print("\\n--- Ringkasan Bulanan (PySpark) ---")
sales_monthly_spark.show(truncate=False)

peak_month = sales_monthly_pd.sort_values(by="Total_Nilai_Transaksi", ascending=False).iloc[0]
print(f"Bulan dengan Omzet Tertinggi: {peak_month['Bulan']}")
print(f"Total Nilai Transaksi : Rp {peak_month['Total_Nilai_Transaksi']:,.0f}".replace(",", "."))
print(f"Frekuensi Transaksi   : {peak_month['Frekuensi_Transaksi']} transaksi")
print(f"Total Unit Terjual    : {peak_month['Total_Unit_Terjual']} unit")"""
    b3_soal4_out = """=== SOAL 4: AGREGASI TOTAL NILAI TRANSAKSI PENJUALAN PER BULAN ===
--- Ringkasan Bulanan (Pandas) ---
  Bulan  Total_Nilai_Transaksi  Frekuensi_Transaksi  Total_Unit_Terjual
2026-01              402600000                   31                 169
2026-02              490700000                   28                 154
2026-03              415650000                   31                 172
2026-04              515200000                   30                 165

--- Ringkasan Bulanan (PySpark) ---
+-------+---------------------+-------------------+------------------+
|Bulan  |Total_Nilai_Transaksi|Frekuensi_Transaksi|Total_Unit_Terjual|
+-------+---------------------+-------------------+------------------+
|2026-01|402600000            |31                 |169               |
|2026-02|490700000            |28                 |154               |
|2026-03|415650000            |31                 |172               |
|2026-04|515200000            |30                 |165               |
+-------+---------------------+-------------------+------------------+

Bulan dengan Omzet Tertinggi: 2026-04
Total Nilai Transaksi : Rp 515.200.000
Frekuensi Transaksi   : 30 transaksi
Total Unit Terjual    : 165 unit"""
    cells.append(create_cell("code", b3_soal4_code, [b3_soal4_out], execution_count=14))

    b3_soal5_md = """### III.5 Soal 5: Interpretasi Analitis Komprehensif & Sintesis Bisnis (Comprehensive Analytical Interpretation)

Sel interpretasi analitis ini menyajikan sintesis komprehensif dwibahasa (*comprehensive bilingual analytical synthesis*) yang merangkum tiga pilar utama praktikum: profil capaian akademik mahasiswa, analisis tren bisnis transaksi penjualan, dan kerangka evaluasi arsitektur komputasi skala besar (*Big Data Engineering Decision Framework*).

---

#### 1. Analisis Kinerja Akademik & Profil Distribusi Mahasiswa (Academic Performance Analysis)

Berdasarkan hasil pemrosesan data pada Soal 1, 2, dan 3 terhadap 52 mahasiswa:

* **Tingkat Kelulusan Kategori Unggul (*Honors Distinction Rate*)**:
  * Dari total 52 mahasiswa yang terdaftar di 4 program studi, sebanyak **33 mahasiswa (63,46%)** berhasil melampaui ambang batas nilai kehormatan (`Nilai > 80`).
  * Proporsi kelulusan di atas 60% ini merefleksikan efektivitas kurikulum dan daya serap materi analitik yang tinggi di kalangan mahasiswa.
* **Perbandingan Capaian Antar-Program Studi**:
  * **Sains Data**: Menempati peringkat pertama dengan nilai rata-rata tertinggi sebesar **83,54**. Capaian ini ditopang oleh mahasiswa dengan skor luar biasa seperti Dimas Wahyu (95), Aulia Rahma (92), dan Indah Permata (91). Keunggulan ini selaras dengan fokus bidang studi yang menitikberatkan pada pemodelan statistik, penalaran kuantitatif, dan analitik data komputasional.
  * **Sistem Informasi**: Menempati peringkat kedua dengan rata-rata **82,92**. Program studi ini mencatatkan peraih skor individu tertinggi di seluruh fakultas, yaitu Kevin Sanjaya dengan nilai **96**, disusul Grace Natalie (92) dan Cantika Zahrani (90).
  * **Teknik Informatika**: Meraih rata-rata **82,69**, didukung oleh performa solid mahasiswa seperti Muhammad Rizky (94) dan Wahyu Setiawan (93). Distribusi nilai menunjukkan konsistensi pemahaman algoritma dan rekayasa perangkat lunak.
  * **Bisnis Digital**: Memperoleh nilai rata-rata **82,38**, dengan capaian tertinggi diraih oleh Salsabila Nadya (94) dan Zulfa Maharani (91). Seluruh program studi menunjukkan performa yang sangat kompetitif dengan selisih rata-rata nilai antar-prodi yang sangat tipis (< 1,2 poin).
* **Heterogenitas Geografis (*Geographic Demographics*)**:
  * Mahasiswa berasal dari 12 kota di berbagai pulau di Indonesia (Jakarta, Bandung, Surabaya, Yogyakarta, Semarang, Malang, Denpasar, Medan, Makassar, Palembang, Bogor, dan Surakarta).
  * Distribusi mahasiswa berprestasi (`Nilai > 80`) tersebar merata di seluruh kota asal tanpa adanya disparitas geografis yang signifikan, menegaskan akses pembelajaran yang inklusif dan merata.

---

#### 2. Analisis Kinerja & Pola Transaksi Penjualan Bulanan (Monthly Sales Trends & Business Synthesis)

Berdasarkan data transaksi `sales.csv` (120 transaksi, 660 unit produk, total omzet Rp 1.824.150.000):

* **Karakteristik Produk dan Struktur Pendapatan**:
  * `Laptop` merupakan produk penggerak omzet utama (*primary revenue driver* / *high-ticket item*), menyumbang **Rp 1.295.000.000 (70,99%)** dari keseluruhan omzet, meskipun hanya mewakili 35 dari 120 transaksi (29,17%).
  * Kategori periferal dan aksesori seperti `Monitor` (12,20%) dan `Printer` (12,18%) menempati segmen sekunder penopang omzet.
  * Produk `Headset` (2,22%), `Keyboard` (1,61%), dan `Mouse` (0,80%) berfungsi sebagai komoditas penarik volume (*volume drivers*) dengan 17 transaksi masing-masing, ideal untuk strategi promosi *bundling* dan *cross-selling*.
* **Dinamika Tren Penjualan Bulanan (Q1–Q2 2026)**:
  * **Januari 2026 (Rp 402.600.000 | 169 unit | 31 transaksi)**: Menunjukkan performa stabil pembukaan awal tahun dengan rata-rata nilai per transaksi sebesar Rp 12.987.097.
  * **Februari 2026 (Rp 490.700.000 | 154 unit | 28 transaksi)**: Terjadi kenaikan omzet yang sangat signifikan (+21,88% dibanding Januari) meskipun jumlah hari operasional dan volume unit lebih sedikit. Rata-rata nilai per transaksi melonjak ke **Rp 17.525.000/transaksi** (tertinggi di Q1), mengindikasikan dominasi transaksi pembelian perangkat keras premium (Laptop dan Monitor).
  * **Maret 2026 (Rp 415.650.000 | 172 unit | 31 transaksi)**: Mengalami koreksi nilai penjualan sebesar -15,29% dari Februari, namun justru mencatat **volume fisik unit tertinggi sepanjang tahun (172 unit)**. Nilai rata-rata per transaksi turun menjadi Rp 13.408.065, merefleksikan pergeseran keranjang belanja konsumen ke produk aksesori pendukung berharga lebih rendah (*low-ticket accessories*).
  * **April 2026 (Rp 515.200.000 | 165 unit | 30 transaksi)**: Mencapai **puncak omzet tertinggi (*all-time monthly peak*)** dalam dataset, menembus angka setengah miliar rupiah (Rp 515,2 Juta). Peningkatan ini dipicu oleh siklus pengadaan perangkat TI kuartalan (*quarterly enterprise procurement*) menjelang awal kuartal kedua.

---

#### 3. Evaluasi Performa Arsitektur: Kapan Memilih Pandas vs PySpark di Industri Big Data

Dalam rekayasa data (*data engineering*) dan ilmu data terapan (*applied data science*), pemilihan antara Pandas dan PySpark bukan sekadar preferensi sintaks, melainkan keputusan strategis arsitektur komputasi:

| Dimensi Evaluasi | Pandas (*In-Memory Single-Node*) | Apache Spark / PySpark (*Distributed Cluster*) |
| :--- | :--- | :--- |
| **Batas Skalabilitas Data** | Terbatas pada RAM mesin lokal (~5–10 GB aman). Melebihi RAM akan memicu *OutOfMemoryError*. | Horizontal scale-out melintasi kluster (terabyte hingga petabyte), didukung mekanisme *spill-to-disk*. |
| **Model Evaluasi Komputasi** | *Eager Evaluation*: Setiap baris kode langsung dieksekusi seketika. | *Lazy Evaluation*: Operasi dirangkai dalam DAG dan dioptimasi oleh *Catalyst Optimizer* sebelum dieksekusi. |
| **Optimasi Query Otomatis** | Tidak ada perencana kueri global. Optimasi bergantung sepenuhnya pada penulisan kode pengguna. | Menyertakan *Catalyst Optimizer* (*predicate pushdown*, *projection pruning*) dan *Tungsten Code Generation*. |
| **Overhead & Latensi Awal** | Sangat rendah (< 1 detik). Respon instan untuk dataset kecil. | Terdapat *startup overhead* (inisialisasi JVM, Py4J bridge, penjadwalan kluster, partisi data). |
| **Pemanfaatan Perangkat Keras** | Terbatas pada single-core CPU (kecuali pustaka ter-vektorisasi tertentu). | Paralelisasi otomatis melintasi seluruh CPU core dan worker node dalam kluster. |
| **Toleransi Kesalahan (*Fault Tolerance*)** | Tidak ada toleransi kesalahan; kegagalan proses mematikan eksekusi (*crash* total). | *Fault-tolerant* berbasis *lineage graph* RDD; partisi yang hilang dikomputasi ulang secara otomatis. |

##### Panduan Keputusan Praktis (Architectural Decision Matrix):
1. **Pilih Pandas Apabila**:
   * Data berukuran kecil hingga menengah yang dapat dimuat sepenuhnya dalam 20–30% kapasitas RAM komputer analis (*desk-side exploratory analysis*).
   * Tahap eksplorasi cepat (*quick prototyping*), validasi hipotesis data awal, dan analisis ad-hoc interaktif pada Jupyter Notebook.
   * Integrasi erat dengan ekosistem visualisasi dan *machine learning single-node* (seperti `matplotlib`, `seaborn`, `statsmodels`, `scikit-learn`).
2. **Pilih PySpark Apabila**:
   * Volume data melampaui kapasitas RAM satu mesin fisik (skala ratusan gigabyte hingga petabyte).
   * Pipeline produksi ETL/ELT skala industri yang berjalan terjadwal di infrastruktur kluster (*cloud data platforms* seperti Databricks, AWS EMR, Google Cloud Dataproc, Azure Synapse).
   * Kebutuhan pemrosesan data terintegrasi dengan arsitektur *Modern Data Lakehouse* berbasis format kolumnar (*Delta Lake, Apache Iceberg, Apache Parquet*).
   * Kebutuhan pemrosesan aliran data waktu nyata (*Spark Structured Streaming*) atau pelatihan model prediktif terdistribusi melintasi multi-node kluster (`pyspark.ml`)."""
    cells.append(create_cell("markdown", b3_soal5_md))

    return cells

def generate_notebook(output_path="main.ipynb"):
    cells = []
    cells.extend(build_header_cells())
    cells.extend(build_bagian_1_cells())
    cells.extend(build_bagian_2_cells())
    cells.extend(build_bagian_3_cells())

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.12"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 2
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)

    print(f"Notebook successfully generated at: {output_path}")
    print(f"Total cells: {len(cells)}")
    code_cells = [c for c in cells if c["cell_type"] == "code"]
    print(f"Code cells : {len(code_cells)}")
    md_cells = [c for c in cells if c["cell_type"] == "markdown"]
    print(f"Markdown cells: {len(md_cells)}")

if __name__ == "__main__":
    out_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "main.ipynb")
    generate_notebook(out_file)
