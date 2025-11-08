import time
import logging
import subprocess
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from hdfs import InsecureClient

# --- Impostazioni ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CASSANDRA_HOST = 'cassandra-seed'
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_DIR = '/iot-data'

# Comandi CQL da eseguire
CQL_CREATE_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS iot_keyspace
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 };
"""

CQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS iot_keyspace.sensor_data (
  sensor_id TEXT,
  timestamp TIMESTAMP,
  temp FLOAT,
  PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
"""

def initialize_cassandra():
    """
    Si connette al cluster (senza keyspace) ed esegue i comandi CQL
    per creare il keyspace e la tabella.
    """
    log.info("Avvio inizializzazione Cassandra...")
    session = None
    cluster = None
    while not session:
        try:
            # Ci connettiamo senza specificare un keyspace
            # Nota: La policy è per evitare i warning visti nei log
            cluster = Cluster(
                [CASSANDRA_HOST],
                port=9042,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
            )
            session = cluster.connect()
            log.info("Connesso al cluster Cassandra (system).")
        except Exception as e:
            log.warning(f"Attesa per il cluster Cassandra... ({e})")
            time.sleep(5)

    try:
        # 1. Crea il Keyspace
        log.info("Esecuzione: Creazione Keyspace 'iot_keyspace'")
        session.execute(CQL_CREATE_KEYSPACE)
        
        # 2. Crea la Tabella
        log.info("Esecuzione: Creazione Tabella 'sensor_data'")
        session.execute(CQL_CREATE_TABLE)
        
        log.info("Keyspace 'iot_keyspace' e tabella 'sensor_data' creati/verificati.")
    
    except Exception as e:
        log.error(f"Errore durante l'esecuzione di CQL: {e}")
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()


def main():
    """
    Script di avvio:
    1. Inizializza Cassandra (crea keyspace/tabella)
    2. (Opzionale: potresti aggiungere qui l'init di HDFS)
    """
    
    # Diamo un po' di tempo ai servizi dipendenti per avviarsi
    log.info("Attesa di 15s per l'avvio dei servizi (Hadoop/Cassandra)...")
    time.sleep(15)

    initialize_cassandra()
    
    log.info("--- Inizializzazione completata ---")
    
    # ---- (MODIFICA QUI) ----
    # Rimuovi o commenta le righe seguenti:
    
    # log.info("Avvio di producer.py...")
    # try:
    #     # Usiamo 'exec' per sostituire questo script con quello del producer
    #     subprocess.run(["python", "producer.py"], check=True)
    # except subprocess.CalledProcessError as e:
    #     log.error(f"producer.py ha fallito con codice {e.returncode}")
    # except FileNotFoundError:
    #     log.error("Errore: 'producer.py' non trovato. Assicurati che sia nella stessa cartella.")
    
    log.info("Script di inizializzazione terminato con successo.") # <-- Puoi aggiungere questo

if __name__ == "__main__":
    main()