# Assignment Brief: Week 3 - Practical Pandas & PySpark Analytics

| Field | Details |
| :--- | :--- |
| **Course** | Big Data Analytics |
| **Assignment** | Week 3: Practical Pandas and PySpark |
| **Author / Student** | [TODO: Student Name / ID] |
| **Status** | Active |
| **Due Date** | [TODO: Due Date] |
| **Original Request** | [original-request.brief.md](./original-request.brief.md) |

---

## 1. Problem / Task Overview

Provide a concise description of the assignment problem, background, core objectives, and scope:

- **Background**:
  Dalam analisis *Big Data*, pemrosesan data mentah menjadi wawasan (*insights*) membutuhkan pemahaman komparatif antara alat pemrosesan *single-machine* (Pandas) dan mesin komputasi terdistribusi (*distributed computing engine* via Apache Spark / PySpark). Praktikum ini mencakup eksplorasi dasar, manipulasi data tabular, agregasi bisnis, serta transisi paradigma dari Pandas ke PySpark.
- **Objectives**:
  1. Membangun lingkungan komputasi analitik (termasuk inisialisasi `SparkSession`).
  2. Melakukan operasi pemrosesan data dasar: membaca dataset (`sales.csv`), inspeksi skema, seleksi kolom, penambahan kolom kalkulasi (`Total = Jumlah * Harga`), dan agregasi tren penjualan bulanan.
  3. Menyelesaikan 5 butir soal **Latihan Mandiri** (pembuatan data mahasiswa minimal 50 baris, filter kondisional nilai > 80, agregasi rata-rata per program studi, agregasi transaksi bulanan berbasis file CSV, dan interpretasi analitis).
  4. Menyajikan komparasi komprehensif antara eksekusi *eager evaluation* pada Pandas dan *lazy evaluation / DAG execution* pada PySpark.
- **Expected Outcome**:
  Satu notebook interaktif terpadu (`main.ipynb`) yang dieksekusi secara bersih *end-to-end* dengan seluruh keluaran visual/tabel terlihat jelas, disertai narasi interpretasi analitis dwibahasa (Bahasa Indonesia dengan istilah teknis bahasa Inggris).

---

## 2. Deliverables & Required Formats

| Deliverable | Format | Required | Description |
| :--- | :--- | :--- | :--- |
| **Main Practice Notebook** | `main.ipynb` | Yes | Notebook gabungan berisi implementasi lengkap Pandas, PySpark, dan Latihan Mandiri 1–5 beserta sel teks/markdown interpretasi hasil. |
| **Project Dependencies** | `requirements.txt` | Yes | Daftar pustaka dan dependensi Python untuk replikasi lingkungan lokal/Colab secara mandiri (*standalone bootstrapping*). |
| **Transactional Dataset** | `sales.csv` | Yes | Dataset transaksi penjualan yang digunakan sebagai input analisis praktikum. |
| **Assignment Brief** | `Brief.md` | Yes | Dokumen spesifikasi teknis dan panduan pengerjaan tugas. |
| **Original Request** | `original-request.brief.md` | Yes | Catatan rekaman instruksi asli dari pengumuman dosen/Google Classroom. |

---

## 3. Data & Resource References

- **Assignment Dataset**:
  - `active/big-data/week-3/sales.csv`
  - Struktur Kolom: `Tanggal` (string format YYYY-MM-DD), `Produk` (string kategori item), `Jumlah` (integer kuantitas item terjual), `Harga` (integer harga satuan item).
- **Course Material References**:
  - `resources/big-data/Week 2 - Modul Session Pandas and Spark.pdf` (*Modul Latihan Awal Big Data Analytics dengan Google Colab: Pandas & PySpark* oleh Anita Safitri).
  - `resources/big-data/Week 2 - Big Data Analytic Pandas and Spark.pptx`.

---

## 4. Instructions & Guidelines

### 4.1. Setup & Environment
1. Python versi 3.10+ disarankan.
2. Dependensi utama: `pandas`, `pyspark`, `jupyter`/`ipykernel`.
3. Folder disiapkan sebagai repositori modular mandiri yang menyertakan `requirements.txt` untuk kemudahan *bootstrapping* di Google Colab maupun *local virtual environment*.

### 4.2. Implementation Workflow (`main.ipynb`)
Notebook harus disusun secara berurutan (*logical progression*) dengan struktur bab sebagai berikut:

#### Bagian I: Lingkungan Kerja & Persiapan Data
- Import library (Pandas, PySpark SQL functions, SparkSession).
- Inisialisasi `SparkSession` lokal (misal: `.appName("BigData-Week3").getOrCreate()`).
- Verifikasi versi paket dan ketersediaan dataset `sales.csv`.

#### Bagian II: Praktikum Terpandu (Guided Walkthrough)
1. **Inspeksi & Pembacaan Data**:
   - Membaca `sales.csv` menggunakan Pandas DataFrame dan Spark DataFrame.
   - Menampilkan skema (`df.info()`, `df.printSchema()`) serta 5–10 baris pertama.
2. **Transformasi & Manipulasi Kolom**:
   - Menambahkan kolom `Total` (`Jumlah * Harga`).
   - Melakukan konversi format tanggal dan mengekstraksi komponen bulan (`Bulan`).
3. **Agregasi & Analisis Penjualan**:
   - Menghitung total nilai penjualan seluruh transaksi.
   - Menghitung total unit terjual dan nilai penjualan per produk.
   - Mengidentifikasi produk dengan kontribusi penjualan terbesar.
   - Menghitung ringkasan transaksi dan tren penjualan per bulan.
4. **Analisis Komparatif Sintaks & Paradigma**:
   - Komparasi cara penulisan operasi filtering, manipulasi kolom (`withColumn` vs direktori kolom Pandas), dan agregasi (`groupBy` vs `groupby`).
   - Penjelasan teknis mengenai perbedaan *in-memory single node* (Pandas) vs *distributed cluster abstraction* (PySpark).

#### Bagian III: Latihan Mandiri (Independent Exercises 1–5)
1. **Soal 1 (Dataset Mahasiswa 50+ Baris)**:
   - Membuat DataFrame berisi data simulasi mahasiswa dengan minimal 50 record dan 4 kolom: `Nama`, `Prodi`, `Nilai`, `Kota`.
2. **Soal 2 (Filter Kondisional)**:
   - Melakukan filtering data untuk menampilkan hanya mahasiswa yang memiliki `Nilai > 80`.
3. **Soal 3 (Agregasi Program Studi)**:
   - Menghitung rata-rata nilai mahasiswa pada setiap program studi (`Prodi`) serta jumlah mahasiswa per prodi.
4. **Soal 4 (Analisis Transaksi Bulanan)**:
   - Membaca dataset transaksi `sales.csv` dan mengelompokkan total nilai transaksi berdasarkan bulan.
5. **Soal 5 (Interpretasi Hasil & Sintesis Bisnis)**:
   - Menuliskan interpretasi terperinci terhadap seluruh output, tren metrik, dan perbedaan arsitektur pemrosesan data.

### 4.3. Formatting & Narrative Rules
- Seluruh interpretasi analitis ditulis menggunakan sel Markdown dalam format dwibahasa (Bahasa Indonesia dengan istilah teknis Bahasa Inggris).
- Setiap blok hasil/tabel wajib disertai penjelasan:
  - Apa yang ditunjukkan oleh output angka/tabel.
  - Hubungan temuan dengan pola penjualan atau karakteristik akademik mahasiswa.
  - Implikasi komputasi (efisiensi, skalabilitas, memori).

---

## 5. Submission Checklist

Sebelum pengumpulan atau pengarsipan, verifikasi seluruh butir pemeriksaan berikut:

- [ ] **All code/scripts executed successfully without errors**: Seluruh notebook `main.ipynb` berjalan lancar dari awal hingga akhir (*Kernel -> Restart & Run All*).
- [ ] **All outputs and visualizations clearly visible**: Seluruh tabel, skema Spark, dan ringkasan metrik tampil secara eksplisit di dalam notebook.
- [ ] **Thorough written interpretations and explanations included**: Setiap hasil agregasi dan latihan mandiri dilengkapi ulasan analitis pada sel Markdown.
- [ ] **File format verification (.ipynb)**: File utama tersimpan dengan ekstensi `.ipynb` yang valid (`main.ipynb`).
- [ ] **Environment bootstrap verification**: File `requirements.txt` tersedia dan dapat digunakan untuk replikasi lingkungan.
- [ ] **Due date check**: Pemeriksaan kepatuhan terhadap batas waktu pengumpulan tugas.
