from flask import Flask, jsonify, request
import pandas as pd
import os
from config import CSV_DIR

app = Flask(__name__)


def load_csv(name):
    path = os.path.join(CSV_DIR, name)

    if not os.path.exists(path):
        return None, f"El archivo {name} no existe."

    try:
        df = pd.read_csv(path)
        return df, None
    except Exception as e:
        return None, str(e)


@app.route("/")
def home():
    return jsonify({"message": "Weather MapReduce API funcionando"})


@app.route("/datasets")
def list_datasets():
    files = [f for f in os.listdir(CSV_DIR) if f.endswith(".csv")]
    return jsonify(files)


# LEER UN CSV Y DEVOLVER JSON
@app.route("/dataset/<filename>")
def get_dataset(filename):
    if not filename.endswith(".csv"):
        filename += ".csv"

    df, err = load_csv(filename)
    if err:
        return jsonify({"error": err}), 404

    return df.to_json(orient="records")


# FILTRAR POR FECHA
@app.route("/dataset/<filename>/filter")
def filter_by_date(filename):
    if not filename.endswith(".csv"):
        filename += ".csv"

    start = request.args.get("start")
    end = request.args.get("end")

    df, err = load_csv(filename)
    if err:
        return jsonify({"error": err}), 404

    if "date" not in df.columns:
        return jsonify({"error": "Este dataset no contiene una columna 'date'."}), 400

    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]

    return df.to_json(orient="records")


@app.route("/dataset/<filename>/values")
def get_values(filename):
    if not filename.endswith(".csv"):
        filename += ".csv"

    df, err = load_csv(filename)
    if err:
        return jsonify({"error": err}), 404

    if "value" not in df.columns:
        return jsonify({"error": "Este dataset no tiene una columna 'value'."}), 400

    return jsonify(df["value"].tolist())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
