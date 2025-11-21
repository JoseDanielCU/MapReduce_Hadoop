HDFS_DIR="/data/weather/"
TARGET_NAME="Data.csv"

echo "Archivo a Subir"
read LOCAL_FILE

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: El archivo no existe."
    exit 1
fi

echo "Renombrando archivo a: $TARGET_NAME"
cp "$LOCAL_FILE" "$TARGET_NAME"

echo "Creando carpeta en HDFS (si no existe)..."
hdfs dfs -mkdir -p $HDFS_DIR

echo "Subiendo archivo a HDFS..."
hdfs dfs -put -f "$TARGET_NAME" "$HDFS_DIR"

echo "Archivo subido correctamente a $HDFS_DIR$TARGET_NAME"
hdfs dfs -ls $HDFS_DIR
