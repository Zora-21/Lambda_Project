#/Users/matteo/Documents/Lambda_IoT/dashboard/app.py

import logging
import time
from datetime import datetime, timedelta 
import docker
from flask import Flask, render_template, jsonify
from cassandra.cluster import Cluster
from hdfs import InsecureClient

# Impostazione del logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.INFO)

# --- Variabili di Connessione ---
CASSANDRA_HOST = 'cassandra-seed'
CASSANDRA_KEYSPACE = 'iot_keyspace' # <-- CORREZIONE
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_OUTPUT_FILE = '/iot-output/daily-averages/part-00000' # Risultato MapReduce

app = Flask(__name__)

# Variabili globali per le sessioni
cassandra_session = None
hdfs_client = None

@app.before_request 
def setup_connections():
    """Si connette a Cassandra e HDFS con una politica di re-try."""
    global cassandra_session, hdfs_client
    
    # Connettiti a Cassandra (se non già connesso)
    if not cassandra_session:
        while True:
            try:
                cluster = Cluster([CASSANDRA_HOST], port=9042)
                cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
                log.info("Dashboard: Connesso a Cassandra!") 
                break # Esce dal loop di Cassandra
            except Exception as e:
                log.warning(f"Dashboard: Attesa per Cassandra... ({e})")
                time.sleep(5)

    # Connettiti a HDFS (se non già connesso)
    if not hdfs_client:
        while True:
            try:
                hdfs_client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
                # Fai un test di connessione
                hdfs_client.status('/') 
                log.info("Dashboard: Connesso a HDFS!")
                break # Esce dal loop di HDFS
            except Exception as e:
                log.warning(f"Dashboard: Attesa per HDFS... ({e})")
                time.sleep(5)

@app.route('/')
def index():
    """Renderizza la pagina HTML principale."""
    return render_template('index.html')

@app.route('/data/realtime')
def get_realtime_data():
    """
    Fornisce i dati più recenti (Speed Layer) come API JSON.
    CALCOLA: Media mobile (ultimi 30 campioni) e Stato (Online/Offline).
    """
    global cassandra_session
    if not cassandra_session:
        log.error("La sessione di Cassandra non è inizializzata!")
        return jsonify({"error": "Cassandra session not initialized"}), 500

    sensor_ids = ['A1', 'B1', 'C1']
    latest_data = {}
    
    # Definiamo il limite per lo stato OFFLINE (es. 30 secondi)
    OFFLINE_THRESHOLD = timedelta(seconds=30)

    for sensor_id in sensor_ids:
        try:
            # --- MODIFICATO: Prendiamo 30 campioni invece di 1 ---
            query = "SELECT temp, timestamp FROM sensor_data WHERE sensor_id = %s LIMIT 30"
            rows = cassandra_session.execute(query, [sensor_id])
            
            # Convertiamo subito in una lista per poterla riutilizzare
            row_list = list(rows) 
            
            if row_list:
                # 1. Calcola la media mobile
                temps = [row.temp for row in row_list]
                avg_temp = sum(temps) / len(temps)
                
                # 2. Prendi il timestamp più recente (è il primo grazie a CLUSTERING ORDER)
                latest_timestamp = row_list[0].timestamp
                
                # 3. Controlla lo stato
                time_diff = datetime.now() - latest_timestamp
                status = "ONLINE" if time_diff < OFFLINE_THRESHOLD else "OFFLINE"
                
                latest_data[sensor_id] = {
                    "temp": round(avg_temp, 2), # Ora questa è la media mobile
                    "timestamp": latest_timestamp.isoformat(),
                    "status": status
                }
            else:
                # Nessun dato trovato per questo sensore
                latest_data[sensor_id] = {
                    "temp": "N/A", 
                    "timestamp": "N/A",
                    "status": "OFFLINE"
                }
        
        except Exception as e:
            log.error(f"Errore query Cassandra per {sensor_id}: {e}")
            latest_data[sensor_id] = {"error": str(e), "status": "ERROR"}

    return jsonify(latest_data)

@app.route('/data/batch')
def get_batch_data():
    """Fornisce i dati storici (Batch Layer) leggendo da HDFS."""
    global hdfs_client
    if not hdfs_client:
        log.error("Il client HDFS non è inizializzato!")
        return jsonify({"error": "HDFS client not initialized"}), 500

    batch_data = {}
    try:
        status = hdfs_client.status(HDFS_OUTPUT_FILE, strict=False)
        
        if status:
            with hdfs_client.read(HDFS_OUTPUT_FILE, encoding='utf-8') as reader:
                for line in reader:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # --- MODIFICATO: Leggiamo il nuovo formato ---
                    try:
                        key, values_str = line.split('\t', 1)
                        avg_temp, min_temp, max_temp = values_str.split('|')
                        
                        batch_data[key] = {
                            "avg": float(avg_temp),
                            "min": float(min_temp),
                            "max": float(max_temp)
                        }
                    except Exception as e:
                        log.warning(f"Riga batch malformata: {line} ({e})")
                    # --- Fine Modifica ---
            
            return jsonify(batch_data)
        else:
            return jsonify({"status": "Batch job results not found. Run the MapReduce job."})

    except Exception as e:
        log.error(f"Errore lettura dati HDFS: {e}")
        return jsonify({"error": f"Failed to read HDFS data: {e}"}), 500

@app.route('/trigger-job', methods=['POST'])
def trigger_batch_job():
    """
    Si connette al Docker socket e avvia 'bash /app/run_job.sh'
    all'interno del container 'nodemanager'.
    """
    try:
        log.info("Ricevuta richiesta di avvio job...")
        # Connettiti al Docker daemon (tramite il socket montato)
        client = docker.from_env()
        
        # Trova il container 'nodemanager'
        try:
            nodemanager = client.containers.get('nodemanager')
        except docker.errors.NotFound:
            log.error("Container 'nodemanager' non trovato.")
            return jsonify({"error": "Container 'nodemanager' non trovato."}), 404

        # Esegui il comando in background (detached)
        # Nota: 'detach=True' è come Popen, restituisce subito.
        nodemanager.exec_run(
            cmd="bash /app/run_job.sh",
            detach=True
        )
        
        log.info("Job MapReduce avviato con successo.")
        return jsonify({"status": "Job MapReduce avviato con successo"}), 200

    except Exception as e:
        log.error(f"Errore nell'avvio del job: {e}")
        return jsonify({"error": f"Errore Docker SDK: {e}"}), 500