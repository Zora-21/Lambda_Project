import os
import json
import logging
import docker
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from hdfs import InsecureClient
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - FLASK - %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)

CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cassandra-seed')
CASSANDRA_KEYSPACE = os.environ.get('CASSANDRA_KEYSPACE', 'iot_keyspace')
HDFS_HOST = os.environ.get('HDFS_HOST', 'namenode')
HDFS_PORT = int(os.environ.get('HDFS_PORT', 9870))
HDFS_USER = os.environ.get('HDFS_USER', 'root')

# Percorsi
HDFS_DAILY_OUTPUT = '/iot-output/daily-averages' 
HDFS_STATS_DIR = '/iot-stats/daily-aggregate'
HDFS_DISCARD_STATS_PATH = '/models/discard_stats.json'
HDFS_SUMMARY_DIR = '/iot-stats/daily-summary'

def get_hdfs_client():
    try: return InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER, timeout=5)
    except: return None

# Globali
cluster = None
cassandra_session = None
docker_client = None

# Cache Performance
last_perf_stats = {}
last_perf_time = 0
PERF_CACHE_DURATION = 10 # secondi

def init_cassandra():
    global cluster, cassandra_session
    if cassandra_session: return
    try:
        cluster = Cluster([CASSANDRA_HOST], port=9042, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'))
        cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
    except: pass

def init_docker():
    global docker_client
    if docker_client: return
    try: docker_client = docker.from_env()
    except: pass

# --- ROUTES ---

@app.route('/')
def index(): return render_template('index.html')

@app.route('/data/realtime')
def get_realtime_data():
    init_cassandra()
    sensor_id = request.args.get('sensor_id')
    if not cassandra_session or not sensor_id: return jsonify({"temp": "N/A", "status": "NO_DATA"})
    try:
        row = cassandra_session.execute(f"SELECT temp FROM sensor_data WHERE sensor_id = '{sensor_id}' LIMIT 1").one()
        if row: return jsonify({"temp": row.temp, "status": "ONLINE"})
    except: pass
    return jsonify({"temp": "N/A", "status": "NO_DATA"})

@app.route('/data/realtime/trend')
def get_realtime_trend():
    """
    Recupera i dati a partire dalla MEZZANOTTE di oggi (Visione Giornaliera).
    Aggrega i dati facendo la media per MINUTO.
    """
    init_cassandra()
    sensor_id = request.args.get('sensor_id')
    try:
        # 1. Calcola l'inizio della giornata odierna (UTC 00:00:00)
        today_midnight = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 2. Query: Prendi tutto da mezzanotte in poi
        query = "SELECT timestamp, temp FROM sensor_data WHERE sensor_id = %s AND timestamp >= %s"
        rows = cassandra_session.execute(query, (sensor_id, today_midnight))
        
        # 3. Aggregazione per MINUTO (Downsampling)
        data_by_minute = defaultdict(list)
        
        for r in rows:
            # Tronca i secondi -> raggruppa per minuto
            ts_minute = r.timestamp.replace(second=0, microsecond=0)
            ts_key = ts_minute.isoformat() + 'Z' # Aggiungi Z per UTC esplicito
            data_by_minute[ts_key].append(r.temp)
            
        # 4. Calcola Media per ogni minuto
        data_points = []
        for ts, temps in data_by_minute.items():
            avg_temp = sum(temps) / len(temps)
            data_points.append({"x": ts, "y": round(avg_temp, 2)})
            
        # 5. Ordina per orario (essenziale per il grafico)
        data_points.sort(key=lambda k: k['x'])
        
        return jsonify({"data": data_points})
        
    except Exception as e: 
        log.error(f"Trend Error: {e}")
        return jsonify({"data": []})

@app.route('/data/batch')
def get_batch_data():
    client = get_hdfs_client()
    sensor_id = request.args.get('sensor_id')
    today = datetime.utcnow().strftime('%Y-%m-%d')
    path = f"{HDFS_SUMMARY_DIR}/date={today}/daily_stats.json"
    try:
        with client.read(path, encoding='utf-8') as r:
            for line in r:
                if line.startswith(f"{sensor_id}-"):
                    parts = line.split('\t', 1)
                    if len(parts) > 1:
                        metrics = json.loads(parts[1])
                        return jsonify({today: metrics})
    except Exception: pass
    return jsonify({"status": "Calcolo in corso..."})

@app.route('/data/aggregate_stats')
def get_aggregate_stats():
    client = get_hdfs_client()
    response = {"total_clean": 0, "total_processed": 0, "total_discarded": 0}
    today = datetime.utcnow().strftime('%Y-%m-%d')
    path = f"{HDFS_STATS_DIR}/date={today}/aggregate_stats.json"
    try:
        with client.read(path, encoding='utf-8') as r:
            data = json.load(r)
            response.update(data)
    except: pass
    return jsonify(response)

@app.route('/data/discard_stats')
def get_discard_stats():
    client = get_hdfs_client()
    response = {"total": 0}
    try:
        if client.status(HDFS_DISCARD_STATS_PATH, strict=False):
            with client.read(HDFS_DISCARD_STATS_PATH, encoding='utf-8') as r:
                content = r.read()
                if content:
                    data = json.loads(content)
                    response["total"] = data.get("total", 0)
    except: pass
    return jsonify(response)

@app.route('/data/performance')
def get_perf():
    global last_perf_stats, last_perf_time
    
    # Cache Check
    import time
    if time.time() - last_perf_time < PERF_CACHE_DURATION and last_perf_stats:
        return jsonify(last_perf_stats)

    init_docker()
    if not docker_client: return jsonify({})
    stats = {}
    for name in ['iot-producer', 'dashboard', 'namenode', 'datanode', 'resourcemanager', 'nodemanager', 'cassandra-seed']:
        try:
            c = docker_client.containers.get(name)
            s = c.stats(stream=False)
            mem = s['memory_stats'].get('usage', 0) / 1024**2
            net = s.get('networks', {})
            rx = sum(v['rx_bytes'] for v in net.values()) / 1024**2
            tx = sum(v['tx_bytes'] for v in net.values()) / 1024**2
            stats[name] = {"mem_mb": round(mem, 2), "net_rx_mb": round(rx, 2), "net_tx_mb": round(tx, 2)}
        except: stats[name] = {"mem_mb": 0, "net_rx_mb": 0, "net_tx_mb": 0}
    
    # Update Cache
    last_perf_stats = stats
    last_perf_time = time.time()
    
    return jsonify(stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)