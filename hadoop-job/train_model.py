#!/usr/bin/env python3
import sys
import json
import math

def main():
    temps_by_sensor = {} # Dizionario per raggruppare le temperature

    # Legge tutti i dati da stdin (provenienti da HDFS)
    for line in sys.stdin:
        try:
            data = json.loads(line.strip())
            sensor_id = data.get("sensor_id")
            temp = data.get("temp")

            if sensor_id and temp is not None:
                if sensor_id not in temps_by_sensor:
                    temps_by_sensor[sensor_id] = []
                temps_by_sensor[sensor_id].append(float(temp))

        except Exception:
            pass # Ignora righe malformate

    model = {}

    # Calcola media e deviazione standard per ogni sensore
    for sensor_id, temps in temps_by_sensor.items():
        n = len(temps)
        if n > 4: # Richiede almeno 5 punti dati per un filtro IQR sensato
            
            # --- INIZIO FILTRAGGIO RUMORE (IQR) ---
            
            # 1. Ordina i dati
            temps.sort()
            
            # 2. Calcola Q1 (25°) e Q3 (75°)
            q1_index = int(n * 0.25)
            q3_index = int(n * 0.75)
            q1 = temps[q1_index]
            q3 = temps[q3_index]
            
            iqr = q3 - q1
            
            # 3. Calcola i limiti per il "rumore"
            # (1.5 è un valore standard)
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
            # 4. Filtra i dati
            cleaned_temps = [t for t in temps if lower_bound <= t <= upper_bound]
            
            # --- FINE FILTRAGGIO RUMORE ---

            # 5. Calcola le statistiche SUI DATI PULITI
            n_cleaned = len(cleaned_temps)
            
            if n_cleaned > 1:
                mean = sum(cleaned_temps) / n_cleaned
                variance = sum((x - mean) ** 2 for x in cleaned_temps) / (n_cleaned - 1)
                std_dev = math.sqrt(variance)

                model[sensor_id] = {
                    "mean": round(mean, 2),
                    "std_dev": round(std_dev, 2)
                }
            
            # (Se n_cleaned è 0 o 1, il sensore viene ignorato, il che è corretto)

    # Stampa il modello (come JSON) su stdout
    print(json.dumps(model, indent=2))

if __name__ == "__main__":
    main()