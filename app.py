import os
import re
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect, url_for,
    send_from_directory, abort, flash
)
import pandas as pd

# -------------------
# Config
# -------------------
CSV_PATH = os.environ.get("CONTRACTS_CSV", "contracts.csv")
PDF_DIR = os.environ.get("PDF_DIR", "contracts.pdf")  # can be a folder called 'contracts.pdf'
EDITS_CSV_PATH = os.environ.get("EDITS_CSV", "contracts_edits.csv")
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")

ID_COL = "id_contrato"  # used in views (falls back to df index if missing)

# Columns we consider for clustering
CLUSTER_KEYS = ["cpf_assinante", "nome_assinante", "cnpj_assinante"]

# -------------------
# App
# -------------------
app = Flask(__name__)
app.secret_key = SECRET_KEY

# Load once; if your CSV changes often, swap this to a function that reloads per request.
def load_df() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")
    # utf-8-sig helps if there is a BOM
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str).fillna("")
    # Keep an immutable row pointer
    if "_row_index" not in df.columns:
        df["_row_index"] = range(len(df))
    # Standardize column names we're going to use (strip whitespace)
    df.columns = [c.strip() for c in df.columns]
    print(df)
    return df

DF = load_df()

def get_row_by_keys(cpf: str, nome: str, cnpj: str) -> pd.Series | None:
    # exact match on the triple to choose a seed row
    mask = (
        (DF.get("cpf_assinante", "") == (cpf or "")) &
        (DF.get("nome_assinante", "") == (nome or "")) &
        (DF.get("cnpj_assinante", "") == (cnpj or ""))
    )
    matches = DF[mask]
    if matches.empty:
        return None
    return matches.iloc[0]

def compute_cluster(seed: pd.Series) -> pd.DataFrame:
    """Cluster = all rows that share ANY of (cpf_assinante, nome_assinante, cnpj_assinante) with the seed row."""
    conds = []
    for key in CLUSTER_KEYS:
        if key in DF.columns and seed.get(key, "") != "":
            conds.append(DF[key] == seed[key])
    if not conds:
        return DF.iloc[0:0]  # empty
    mask = conds[0]
    for c in conds[1:]:
        mask = mask | c
    cluster_df = DF[mask].copy()
    print(cluster_df)
    return cluster_df.sort_values(by=[ID_COL] if ID_COL in DF.columns else ["_row_index"])

def sanitize_name(name: str) -> str:
    """Safe HTML field name."""
    # Keep a map so we can reverse it later if ever needed
    return re.sub(r"[^0-9a-zA-Z_]+", "_", name)

# Build a form schema: visible order = CSV order
ALL_COLUMNS = list(DF.columns)
# Put _row_index at the end in forms
if "_row_index" in ALL_COLUMNS:
    ALL_COLUMNS = [c for c in ALL_COLUMNS if c != "_row_index"] + ["_row_index"]

# Map original -> safe
SAFE_NAME_MAP = {col: sanitize_name(col) for col in ALL_COLUMNS}
# Reverse map
UNSAFE_NAME_MAP = {v: k for k, v in SAFE_NAME_MAP.items()}

# -------------------
# Routes
# -------------------

@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")

@app.route("/pick", methods=["POST"])
def pick():
    cpf = (request.form.get("cpf_assinante") or "").strip()
    nome = (request.form.get("nome_assinante") or "").strip()
    cnpj = (request.form.get("cnpj_assinante") or "").strip()

    row = get_row_by_keys(cpf, nome, cnpj)
    if row is None:
        flash("No exact match found for that CPF + Nome + CNPJ trio.", "warning")
        # If there are partial matches, offer them to pick from:
        partial_mask = (
            ((DF.get("cpf_assinante", "") == cpf) |
             (DF.get("nome_assinante", "") == nome) |
             (DF.get("cnpj_assinante", "") == cnpj))
        )
        partial = DF[partial_mask].copy()
        if partial.empty:
            return redirect(url_for("home"))
        options = partial.head(200)  # keep list manageable
        return render_template("pick.html", rows=options.to_dict(orient="records"), id_col=ID_COL)
    else:
        row_index = int(row["_row_index"])
        return redirect(url_for("cluster", row_index=row_index))

@app.route("/cluster/<int:row_index>", methods=["GET"])
def cluster(row_index: int):
    if row_index < 0 or row_index >= len(DF):
        abort(404)
    seed = DF.iloc[row_index]
    cluster_df = compute_cluster(seed)
    seed_id = seed.get(ID_COL, f"row-{row_index}")
    return render_template(
        "cluster.html",
        seed=seed.to_dict(),
        rows=cluster_df.to_dict(orient="records"),
        id_col=ID_COL
    )

@app.route("/contract/<int:row_index>", methods=["GET"])
def contract(row_index: int):
    if row_index < 0 or row_index >= len(DF):
        abort(404)
    row = DF.iloc[row_index]
    filename = (row.get("file_name") or "").strip()
    pdf_exists = os.path.isfile(os.path.join(PDF_DIR, filename)) if filename else False

    # Build (safe_name, label, value) tuples for the form
    fields = []
    for col in ALL_COLUMNS:
        value = row.get(col, "")
        fields.append({
            "safe": SAFE_NAME_MAP[col],
            "label": col,
            "value": "" if pd.isna(value) else str(value)
        })

    return render_template(
        "contract.html",
        row=row.to_dict(),
        row_index=row_index,
        fields=fields,
        pdf_filename=filename,
        pdf_exists=pdf_exists,
        id_col=ID_COL
    )

@app.route("/contract/<int:row_index>/update", methods=["POST"])
def update(row_index: int):
    if row_index < 0 or row_index >= len(DF):
        abort(404)
    # Rehydrate posted data back to original column names
    posted = {}
    for safe_name, val in request.form.items():
        orig = UNSAFE_NAME_MAP.get(safe_name)
        if orig:
            posted[orig] = val

    # Ensure row_index and timestamp present
    posted["_row_index"] = str(row_index)
    posted["_edited_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Write (append) to the edits CSV WITHOUT modifying original
    # Keep deterministic column order: all original columns + metadata
    out_cols = [c for c in ALL_COLUMNS]  # copy
    if "_edited_at" not in out_cols:
        out_cols.append("_edited_at")

    # If some columns are missing from posted (e.g., user removed a field from DOM), fill with original values
    original_row = DF.iloc[row_index].to_dict()
    complete = {c: posted.get(c, str(original_row.get(c, ""))) for c in out_cols}

    # Append line
    exists = os.path.exists(EDITS_CSV_PATH)
    # Write using pandas for simplicity
    pd.DataFrame([complete], columns=out_cols).to_csv(
        EDITS_CSV_PATH, mode="a", index=False, header=not exists, encoding="utf-8-sig"
    )

    flash(f"Changes appended to {EDITS_CSV_PATH}", "success")
    return redirect(url_for("contract", row_index=row_index))

@app.route("/pdf/<path:filename>")
def serve_pdf(filename: str):
    # Serve from the configured directory; disallow path traversal by using send_from_directory
    if not os.path.isdir(PDF_DIR):
        abort(404)
    # If your files are not exactly named as in CSV, adjust here
    return send_from_directory(PDF_DIR, filename, mimetype="application/pdf", max_age=0)

# Helpful 404 page
@app.errorhandler(404)
def not_found(e):
    return render_template("base.html", content="<h2>Not found</h2>"), 404

if __name__ == "__main__":
    app.run(debug=True)
