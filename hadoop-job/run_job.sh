#!/bin/bash

# --- CORREZIONE: Definiamo esplicitamente i percorsi ---
# Definiamo l'home di Hadoop
export HADOOP_HOME=/opt/hadoop-3.2.1

# Definiamo i comandi con il loro percorso completo
HDFS_CMD="$HADOOP_HOME/bin/hdfs"
HADOOP_CMD="$HADOOP_HOME/bin/hadoop"
# --- FINE CORREZIONE ---

# Definiamo l'indirizzo corretto del NameNode
HDFS_URI="hdfs://namenode:9000"

# Nome della directory di output su HDFS
OUTPUT_DIR="/iot-output/daily-averages"

# Nome della directory di input su HDFS
INPUT_DIR="/iot-data/*" 

echo "Avvio del job MapReduce per le medie giornaliere..."

# Rimuove la cartella di output precedente, se esiste
# CORREZIONE: Usiamo il comando completo
$HDFS_CMD dfs -fs $HDFS_URI -rm -r $OUTPUT_DIR

# Esegue il job hadoop-streaming
# CORREZIONE: Usiamo il comando completo
$HADOOP_CMD jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapred.job.name="IoT Daily Temp Avg" \
    -fs $HDFS_URI \
    -files /app/mapper.py,/app/reducer.py \
    -mapper /app/mapper.py \
    -reducer /app/reducer.py \
    -input $INPUT_DIR \
    -output $OUTPUT_DIR

echo "Job completato. Controllo dei risultati:"

# Mostra i primi 10 risultati
# CORREZIONE: Usiamo il comando completo
$HDFS_CMD dfs -fs $HDFS_URI -cat $OUTPUT_DIR/part-00000 | head