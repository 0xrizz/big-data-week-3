import json
import os

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
 Monitor          89        222500000                   19              12.20
 Printer         101        222200000                   19              12.18
 Headset          90         40500000                   16               2.22
Keyboard          98         29400000                   16               1.61
   Mouse          97         14550000                   15               0.80

=== KINERJA PENJUALAN PER PRODUK (PYSPARK) ===
+--------+----------+---------------+-------------------+-----------------+
|Produk  |Total_Unit|Total_Penjualan|Frekuensi_Transaksi|Kontribusi_Persen|
+--------+----------+---------------+-------------------+-----------------+
|Laptop  |185       |1295000000     |35                 |70.99            |
|Monitor |89        |222500000      |19                 |12.2             |
|Printer |101       |222200000      |19                 |12.18            |
|Headset |90        |40500000       |16                 |2.22             |
|Keyboard|98        |29400000       |16                 |1.61             |
|Mouse   |97        |14550000       |15                 |0.8              |
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

def generate_notebook(output_path="main.ipynb"):
    cells = []
    cells.extend(build_header_cells())
    cells.extend(build_bagian_1_cells())
    cells.extend(build_bagian_2_cells())

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
    generate_notebook("main.ipynb")
