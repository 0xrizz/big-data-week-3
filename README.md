# Practical Big Data Analytics: Pandas & PySpark (Week 3)

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-150458.svg)](https://pandas.pydata.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C.svg)](https://spark.apache.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626.svg)](https://jupyter.org/)

A comprehensive hands-on comparative analysis and practical implementation of data processing workflows using **Pandas** (single-node, in-memory) and **Apache Spark / PySpark** (distributed computing engine).

---

## Project Overview

This repository contains the complete practical implementation and independent exercises for **Week 3 Big Data Analytics**. The primary objective is to evaluate differences in execution paradigms, performance trade-offs, and syntax patterns between single-machine data processing (Pandas) and distributed data processing (PySpark).

### Core Highlights
- **Dual Framework Implementation**: Every analysis step is demonstrated using both Pandas and PySpark APIs side-by-side.
- **Transactional Data Analysis**: In-depth analysis of retail transactional data (`sales.csv`) featuring 120 records across 6 technology products.
- **Architectural Comparison**: Deep dive into *Eager Evaluation* vs. *Lazy Evaluation / DAG Execution*, Catalyst Optimizer stages (Logical/Physical plans), and memory scalability models.
- **Independent Problem Sets (*Latihan Mandiri 1–5*)**: Custom 52-student dataset generation, conditional filtering (`Nilai > 80`), multi-metric program study aggregations, and monthly revenue aggregation.
- **Bilingual Academic Commentary**: Detailed analytical findings and business insights written in Indonesian with standard English technical terminology.

---

## Repository Structure

```text
.
├── .gitignore              # Git ignore configuration (virtual environments, caches, and internal briefs)
├── README.md               # Repository documentation and project overview
├── main.ipynb              # Unified notebook containing end-to-end code, executions, and insights
├── requirements.txt        # Standalone Python dependencies for local and Colab execution
└── sales.csv               # Transactional sales dataset (120 records)
```

---

## Analytical Walkthrough & Key Findings

### 1. Guided Walkthrough on `sales.csv`
- **Total Revenue**: Rp 1.824.150.000 across 660 total units sold.
- **Top Product by Revenue**: **Laptop** contributed **Rp 889.000.000 (48.74%)** of total sales, demonstrating high-ticket revenue concentration despite lower unit volume.
- **Volume Drivers**: Accessories (Mouse, Headset, Keyboard) drove transaction volume and basket-building potential.
- **Quarterly Sales Trend**: Sales exhibited healthy cyclical continuity across January, February, and March 2026.

### 2. Paradigm Comparison: Pandas vs. PySpark

| Dimension | Pandas | PySpark |
| :--- | :--- | :--- |
| **Execution Model** | Eager evaluation (immediate in-memory execution) | Lazy evaluation (builds Directed Acyclic Graph / DAG) |
| **Memory Limit** | Bound by single-machine RAM (OOM risk on large data) | Scales out across cluster workers, spills to disk gracefully |
| **Optimization** | Developer-dependent code vectorization | Automated via Catalyst Optimizer and Tungsten Engine |
| **Best Used For** | Exploratory analysis, small-to-medium datasets (< 10 GB) | Large-scale big data pipelines, streaming, enterprise ETL |

### 3. Latihan Mandiri 1–5 (Student Analytics)
- **Dataset**: 52 simulated students across 4 programs: *Sains Data*, *Sistem Informasi*, *Teknik Informatika*, and *Bisnis Digital*.
- **High-Performance Threshold**: 33 students (63.46%) achieved scores $> 80$.
- **Program Benchmark**: *Sains Data* achieved the highest average score (85.23), followed closely by *Teknik Informatika* (84.08).

---

## Getting Started & Execution

### Prerequisites
- Python 3.10 or higher
- Java Runtime Environment (JRE 8, 11, or 17 recommended for PySpark local mode)

### Local Setup
1. **Clone the repository**:
   ```bash
   git clone https://github.com/0xrizz/big-data-week-3.git
   cd big-data-week-3
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Launch the Jupyter Notebook**:
   ```bash
   jupyter notebook main.ipynb
   ```

### Google Colab Execution
To run in Google Colab:
1. Open [Google Colab](https://colab.research.google.com/).
2. Upload `main.ipynb` and `sales.csv`.
3. In the first cell, ensure PySpark is installed:
   ```python
   !pip install pyspark
   ```
4. Run all cells sequentially (**Runtime -> Run all**).

---

## Coursework Metadata

- **Course**: Big Data Analytics
- **Module**: Week 3 - Practical Pandas and PySpark
- **Author**: 0xrizz
- **Status**: Completed & Verified

---

## License

This repository is maintained for academic coursework and portfolio purposes.
