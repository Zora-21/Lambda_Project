#!/bin/bash

# 1. Avvia il demone cron in background
echo "Avvio del demone cron..."
/usr/sbin/cron

# 2. Avvia il tail del log in background (per il debug)
echo "Avvio streaming log di cron..."
tail -f /var/log/cron.log &

# 3. Esegui il comando originale del NodeManager in PRIMO PIANO
echo "Avvio del servizio YARN NodeManager..."
exec yarn nodemanager