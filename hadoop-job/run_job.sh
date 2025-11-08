#!/bin/bash

# --- Imposta JAVA_HOME per cron ---
source /opt/hadoop-3.2.1/etc/hadoop/hadoop-env.sh

export HADOOP_HOME=/opt/hadoop-3.2.1
HDFS_CMD="$HADOOP_HOME/bin/hdfs"
HADOOP_CMD="$HADOOP_HOME/bin/hadoop"
HDFS_URI="hdfs://namenode:9000"

# --- FASE 1: ADDESTRAMENTO MODELLO (PULIZIA) ---
echo "Fase 1: Addestramento modello di pulizia (train_model.py)..."
MODEL_FILE_PATH="/app/model.json" 
$HDFS_CMD dfs -fs $HDFS_URI -cat /iot-data/date=*/*.jsonl | \
  python3 /app/train_model.py > $MODEL_FILE_PATH
echo "Modello salvato localmente in $MODEL_FILE_PATH"
cat $MODEL_FILE_PATH

# --- Carica modello su HDFS per il Producer ---
echo "Caricamento modello aggiornato su HDFS..."
$HDFS_CMD dfs -fs $HDFS_URI -mkdir -p /models
$HDFS_CMD dfs -fs $HDFS_URI -put -f $MODEL_FILE_PATH /models/model.json
echo "Modello caricato in /models/model.json"


# --- FASE 2: JOB MAPREDUCE (PULITO) ---
TODAY_DATE=$(date +%Y-%m-%d)
INPUT_DIR="/iot-data/date=$TODAY_DATE/*"
OUTPUT_DIR="/iot-output/daily-averages/date=$TODAY_DATE"

echo "Fase 2: Avvio job MapReduce (con pulizia) per i dati di OGGI ($TODAY_DATE)..."
$HDFS_CMD dfs -fs $HDFS_URI -rm -r -skipTrash $OUTPUT_DIR

$HADOOP_CMD jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapred.job.name="Crypto Financial Stats (Cleaned) - $TODAY_DATE" \
    -fs $HDFS_URI \
    -files /app/mapper.py,/app/reducer.py,$MODEL_FILE_PATH \
    -mapper "python3 /app/mapper.py" \
    -reducer "python3 /app/reducer.py" \
    -input $INPUT_DIR \
    -output $OUTPUT_DIR

echo "Job completato. Controllo dei risultati (puliti) per $TODAY_DATE:"
$HDFS_CMD dfs -fs $HDFS_URI -cat $OUTPUT_DIR/part-00000 | head

# --- INIZIO MODIFICA: Trigger per la rotazione dei contatori ---
echo "Creazione del file trigger per la rotazione dei contatori..."
# Creiamo un file vuoto
touch /tmp/rotate_trigger_file
# Carichiamolo su HDFS dove il producer può vederlo
$HDFS_CMD dfs -fs $HDFS_URI -put -f /tmp/rotate_trigger_file /models/rotate_trigger
echo "File trigger caricato in /models/rotate_trigger"
# --- FINE MODIFICA ---