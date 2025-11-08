#!/usr/bin/env python3
"""
reducer.py - Reducer avanzato (compatibile Python < 3.6)

Carica 'model.json' per pulire (filtrare) i dati.
Calcola metriche OHLC e statistiche sui dati PULITI.
Aggiunge il conteggio dei dati SCARTATI.

Emette: CHIAVE \t JSON_METRICS
"""

import sys
import json
import math

# --- Carica il modello di pulizia ---
MODEL_FILE = 'model.json'
anomaly_model = {}

try:
    with open(MODEL_FILE, 'r') as f:
        anomaly_model = json.load(f)
except FileNotFoundError:
    print("ATTENZIONE: model.json non trovato. Dati non puliti.", file=sys.stderr)
    pass
except Exception as e:
    # --- MODIFICATO: Rimossa f-string ---
    print("Errore nel caricamento di model.json: {}".format(e), file=sys.stderr)
    pass
# --- Fine ---


# Variabili per tracciare il gruppo corrente
current_key = None
current_values = [] # Lista per (timestamp, temp)

def calculate_metrics_and_print(key, values):
    """
    Funzione helper per calcolare le metriche e stampare il JSON.
    """
    if not values:
        return
    
    try:
        # --- Filtra i dati prima dei calcoli ---
        sensor_id = key.split('-', 1)[0]
        model_params = anomaly_model.get(sensor_id)
        
        cleaned_values = []
        
        if model_params:
            mean = model_params.get('mean')
            std_dev = model_params.get('std_dev')
            
            if mean is not None and std_dev is not None and std_dev > 0:
                lower_bound = mean - (3 * std_dev)
                upper_bound = mean + (3 * std_dev)
                
                for ts, temp in values:
                    if lower_bound <= temp <= upper_bound:
                        cleaned_values.append((ts, temp))
            else:
                cleaned_values = values
        else:
            cleaned_values = values
            
        # --- Fine Filtro ---

        if not cleaned_values:
            print("Nessun dato 'pulito' per {}".format(key), file=sys.stderr)
            return

        # --- INIZIO NUOVA MODIFICA: Conteggio Scartati ---
        total_count = len(values)
        cleaned_count = len(cleaned_values)
        discarded_count = total_count - cleaned_count
        discarded_pct = round((discarded_count / total_count) * 100, 2) if total_count > 0 else 0
        # --- FINE NUOVA MODIFICA ---

        # 1. Ordina i valori PULITI per timestamp
        cleaned_values.sort(key=lambda x: x[0])
        prices = [v[1] for v in cleaned_values]
        
        # 3. Calcola le metriche
        count = len(prices) # Questo è cleaned_count
        if count == 0:
            return
            
        open_price = prices[0]
        close_price = prices[-1]
        min_price = min(prices)
        max_price = max(prices)
        
        daily_change = close_price - open_price
        daily_change_pct = (daily_change / open_price) * 100 if open_price > 0 else 0
        
        price_range = max_price - min_price
        range_pct = (price_range / open_price) * 100 if open_price > 0 else 0
        
        volatility = 0
        if count > 1:
            mean = sum(prices) / count
            variance = sum((x - mean) ** 2 for x in prices) / (count - 1)
            std_dev = math.sqrt(variance)
            
            if mean > 0:
                volatility = (std_dev / mean) * 100
        
        trend = "FLAT"
        if daily_change_pct > 0.5:
            trend = "UP"
        elif daily_change_pct < -0.5:
            trend = "DOWN"

        # 4. Componi l'output JSON
        metrics = {
            "open": round(open_price, 2),
            "close": round(close_price, 2),
            "min": round(min_price, 2),
            "max": round(max_price, 2),
            "count": count, # Osservazioni Pulite
            "total_count": total_count, # Osservazioni Totali
            "discarded_count": discarded_count, # Osservazioni Scartate
            "discarded_pct": discarded_pct,
            "daily_change": round(daily_change, 2),
            "daily_change_pct": round(daily_change_pct, 2),
            "range_pct": round(range_pct, 2),
            "volatility": round(volatility, 2),
            "trend": trend
        }
        
        # 5. Stampa il risultato
        print("{}\t{}".format(key, json.dumps(metrics)))

    except Exception as e:
        print("Errore nel calcolo delle metriche per {}: {}".format(key, e), file=sys.stderr)


# --- Loop principale del Reducer (invariato) ---

for line in sys.stdin:
    try:
        line = line.strip()
        key, value_str = line.split('\t', 1)
        temp, timestamp = value_str.split('|')
        
        temp = float(temp)
        timestamp = int(timestamp)

        if current_key == key:
            current_values.append((timestamp, temp))
        else:
            if current_key:
                calculate_metrics_and_print(current_key, current_values)
            
            current_key = key
            current_values = [(timestamp, temp)]

    except ValueError:
        pass

# Non dimenticare l'ultimo gruppo!
if current_key:
    calculate_metrics_and_print(current_key, current_values)