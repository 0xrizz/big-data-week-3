import json
import os
import sys

def check_bagian_1_and_2(cells):
    source_texts = ["".join(c.get("source", [])) for c in cells]
    has_spark_init = any("SparkSession.builder" in s for s in source_texts)
    has_pandas_read = any("pd.read_csv" in s for s in source_texts)
    has_spark_read = any("spark.read" in s for s in source_texts)
    has_calc_total = any("Jumlah" in s and "Harga" in s for s in source_texts)
    assert has_spark_init, "Missing SparkSession initialization"
    assert has_pandas_read, "Missing pd.read_csv"
    assert has_spark_read, "Missing spark.read.csv"
    assert has_calc_total, "Missing Total column calculation"

def verify_notebook(notebook_path: str) -> dict:
    if not os.path.exists(notebook_path):
        raise FileNotFoundError(f"Notebook file not found: {notebook_path}")

    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    assert nb.get("nbformat") == 4, f"Invalid nbformat version: {nb.get('nbformat')}"
    assert "cells" in nb, "Notebook missing 'cells' key"
    cells = nb["cells"]
    assert len(cells) >= 15, f"Notebook has insufficient cells: {len(cells)}"

    code_cells = [c for c in cells if c.get("cell_type") == "code"]
    for i, cell in enumerate(code_cells):
        assert cell.get("outputs") is not None and len(cell.get("outputs")) > 0, f"Code cell {i+1} has no outputs"
        assert cell.get("execution_count") is not None, f"Code cell {i+1} has no execution_count"

    check_bagian_1_and_2(cells)

    required_keywords = [
        "Bagian I",
        "Bagian II",
        "Bagian III",
        "SparkSession",
        "sales.csv",
        "Total",
        "Latihan Mandiri",
        "Soal 1",
        "Soal 2",
        "Soal 3",
        "Soal 4",
        "Soal 5",
        "Interpretasi"
    ]

    all_source = "".join("".join(c.get("source", [])) for c in cells)
    for kw in required_keywords:
        assert kw in all_source, f"Missing required keyword/section: {kw}"

    print(f"Validation SUCCESS: {len(cells)} total cells ({len(code_cells)} code cells with outputs).")
    return {"total_cells": len(cells), "code_cells": len(code_cells)}

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "active/big-data/week-3/main.ipynb"
    try:
        summary = verify_notebook(path)
        print(f"Summary: {summary}")
    except Exception as e:
        print(f"Validation FAILED: {e}", file=sys.stderr)
        sys.exit(1)
