#!/usr/bin/env python3
"""
reducer.py - Calcola media, minimo e massimo.
Emette: CHIAVE \t MEDIA|MIN|MAX
"""

import sys

current_key = None
current_sum = 0
current_count = 0
# --- AGGIUNTO ---
current_min = float('inf') # Inizia con un valore infinito positivo
current_max = float('-inf') # Inizia con un valore infinito negativo

for line in sys.stdin:
    try:
        line = line.strip()
        key, value = line.split('\t', 1)
        temp = float(value)

        if current_key == key:
            current_sum += temp
            current_count += 1
            # --- AGGIUNTO ---
            current_min = min(current_min, temp)
            current_max = max(current_max, temp)
        else:
            if current_key:
                average = current_sum / current_count
                # --- MODIFICATO: Emette tutti e 3 i valori separati da | ---
                print("{}\t{:.2f}|{:.2f}|{:.2f}".format(current_key, average, current_min, current_max))

            # Resetta i contatori per la nuova chiave
            current_key = key
            current_sum = temp
            current_count = 1
            # --- AGGIUNTO ---
            current_min = temp
            current_max = temp

    except ValueError:
        pass

# Non dimenticare l'ultimo gruppo!
if current_key:
    average = current_sum / current_count
    # --- MODIFICATO ---
    print("{}\t{:.2f}|{:.2f}|{:.2f}".format(current_key, average, current_min, current_max))