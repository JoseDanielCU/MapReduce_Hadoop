
MAPREDUCE_DIR="./MapReduce"
HDFS_DATA="hdfs:///data/weather/Data.csv"

echo "Lista de Jobs disponibles:"
ls $MAPREDUCE_DIR/*.py

echo "Escribe el nombre del Job (sin ruta):"
read JOB

JOB_PATH="$MAPREDUCE_DIR/$JOB"

if [ ! -f "$JOB_PATH" ]; then
    echo "El job no existe."
    exit 1
fi

echo "Ingresa el nombre del directorio de salida en HDFS:"
echo "(Ejemplo: avg_temp, daily_stats, humedad)"
read OUTPUT

OUTPUT_DIR="hdfs:///results/$OUTPUT"

echo "¿Deseas agregar parámetros como --start y --end? (s/n)"
read ADD_PARAMS

if [ "$ADD_PARAMS" == "s" ]; then
    echo "Introduce los parámetros completos, ejemplo:"
    echo "--start 2022-01-01 --end 2022-01-10"
    read PARAMS
else
    PARAMS=""
fi

echo "Ejecutando Job..."
python3 $JOB_PATH \
    $HDFS_DATA \
    -r hadoop \
    --output-dir $OUTPUT_DIR \
    $PARAMS

echo "Job ejecutado con éxito. Output en:"
echo "$OUTPUT_DIR"
