# Archivo: pig/Dockerfile
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

ENV PIG_VERSION=0.17.0

# --- CORRECCIÓN DE REPOSITORIOS (Debian Stretch es EOL) ---
# Cambiamos los repositorios a archive.debian.org y desactivamos el chequeo de validez de fecha
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get -o Acquire::Check-Valid-Until=false install -y wget && \
    wget https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz && \
    tar -xzf pig-$PIG_VERSION.tar.gz -C /usr/local/ && \
    mv /usr/local/pig-$PIG_VERSION /usr/local/pig && \
    rm pig-$PIG_VERSION.tar.gz && \
    apt-get remove -y wget && \
    apt-get autoremove -y && \
    apt-get clean

# Configurar variables de entorno
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV PIG_CLASSPATH=$HADOOP_CONF_DIR

# Mantener el contenedor vivo
CMD ["tail", "-f", "/dev/null"]