#!/bin/bash

LOCAL_RESULTS="../Results_CSV"
mkdir -p $LOCAL_RESULTS

echo "Ingresa el nombre del folder dentro de /data/weather/Output/ que quieres descargar:"
read FOLDER

HDFS_DIR="/data/weather/Output/$FOLDER"
LOCAL_RAW="${FOLDER}_raw.txt"
LOCAL_CSV="${FOLDER}.csv"

echo "Descargando y uniendo partes..."
hdfs dfs -cat $HDFS_DIR/part-* > $LOCAL_RAW

if [ ! -f "$LOCAL_RAW" ]; then
    echo "Error: no se pudo generar $LOCAL_RAW"
    exit 1
fi

echo "Convirtiendo a CSV y ordenando..."

python3 - <<EOF
import csv

raw_file = "$LOCAL_RAW"
csv_out = "$LOCAL_RESULTS/$LOCAL_CSV"

rows = []

with open(raw_file) as fin:
    for line in fin:
        line = line.strip()
        if not line:
            continue

        try:
            key_part, val_part = line.split("\t")
        except:
            continue

        key = key_part.replace('"', '').strip()
        value = float(val_part.strip())

        rows.append((key, value))

rows.sort(key=lambda x: x[0])

with open(csv_out, "w", newline="") as fout:
    writer = csv.writer(fout)
    writer.writerow(["date", "value"])
    writer.writerows(rows)
EOF

echo "Archivo final generado: $LOCAL_RESULTS/$LOCAL_CSV"
