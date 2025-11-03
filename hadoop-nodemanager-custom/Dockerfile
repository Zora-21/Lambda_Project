# Partiamo dall'immagine originale del nodemanager
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

# Passiamo a root per installare pacchetti
USER root

# CORREZIONE: Puntiamo ai repository di archivio "Stretch" (EOL)
RUN echo "deb http://archive.debian.org/debian/ stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security/ stretch/updates main" >> /etc/apt/sources.list

# Aggiorniamo e installiamo python3 (ignorando gli avvisi sui vecchi pacchetti)
RUN apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --allow-unauthenticated python3