#!/bin/bash

LOCAL_RESULTS="./results_csv"
mkdir -p $LOCAL_RESULTS

echo "Ingresa el nombre del folder en /results/ que quieres descargar:"
read FOLDER

HDFS_FILE="/results/$FOLDER/part-00000"
LOCAL_RAW="${FOLDER}_raw.txt"
LOCAL_CSV="${FOLDER}.csv"

echo "Descargando archivo desde HDFS..."
hdfs dfs -get -f $HDFS_FILE $LOCAL_RAW

if [ ! -f "$LOCAL_RAW" ]; then
    echo "Error al descargar $HDFS_FILE"
    exit 1
fi

echo "Convirtiendo a CSV..."

python3 - <<EOF
import csv, json

raw = "$LOCAL_RAW"
csv_out = "$LOCAL_RESULTS/$LOCAL_CSV"

with open(raw) as fin, open(csv_out, "w", newline="") as fout:
    writer = csv.writer(fout)
    writer.writerow(["key", "value"])
    for line in fin:
        try:
            key, value = json.loads(line)
            writer.writerow([key, value])
        except:
            pass
EOF

echo "Archivo convertido: $LOCAL_RESULTS/$LOCAL_CSV"
