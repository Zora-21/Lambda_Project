#!/usr/bin/env python3
"""
mapper.py - Script Map avanzato per analisi finanziarie.

Legge i log JSON dei sensori da stdin, estrae il sensor_id,
la data (dal timestamp), la temperatura e il timestamp completo.

Emette una coppia chiave-valore separata da tab:
CHIAVE: <sensor_id>-<data>
VALORE: <temperatura>|<timestamp_unix>

Il timestamp unix permette al reducer di calcolare la sequenza temporale
e metriche come volatilità e trend.
"""

import sys
import json
from datetime import datetime

for line in sys.stdin:
    try:
        # Rimuove spazi bianchi e carica il JSON
        line = line.strip()
        data = json.loads(line)

        # Estrae i dati necessari
        sensor_id = data.get("sensor_id")
        temp = data.get("temp")
        timestamp_str = data.get("timestamp")  # Formato ISO: 2025-10-31T10:00:00.123

        # Assicurati che tutti i dati siano presenti
        if sensor_id and temp is not None and timestamp_str:
            # Estrae solo la parte della data (YYYY-MM-DD)
            date_str = timestamp_str.split('T')[0]
            
            # Converte timestamp in unix per ordinamento
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                timestamp_unix = int(dt.timestamp())
            except:
                # Fallback: usa solo la stringa
                timestamp_unix = 0
            
            # Crea la chiave composita
            output_key = "{}-{}".format(sensor_id, date_str)
            
            # Emette temp|timestamp per permettere ordinamento e calcoli avanzati
            output_value = "{}|{}".format(float(temp), timestamp_unix)
            
            # Emette la coppia chiave-valore su stdout
            print("{}\t{}".format(output_key, output_value))

    except Exception as e:
        # Ignora le righe malformate
        pass