FROM apache/spark:3.5.1
USER root

# System packages
RUN apt-get update && \
    apt-get install -y python3-pip python3-dev build-essential vim nano bash-completion && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Bash completion
RUN echo ". /usr/share/bash-completion/bash_completion" >> /etc/bash.bashrc

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
# Python packages — split heavy ones separately for better caching
# RUN pip3 install --no-cache-dir numpy pandas ipython
# RUN pip3 install --no-cache-dir deltalake
# RUN pip3 install --no-cache-dir duckdb==0.9.2
# RUN pip3 install --no-cache-dir pyspark jupyterlab
# RUN pip3 install --no-cache-dir delta-spark==3.1.0

# Download JARs
RUN curl -o /opt/spark/jars/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -o /opt/spark/jars/delta-storage-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# Directories and permissions
RUN mkdir -p /home/spark/.local/share/jupyter/runtime && \
    mkdir -p /opt/spark-notebooks && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark-notebooks

USER spark
ENV SHELL /bin/bash
WORKDIR /opt/spark-notebooks
EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--NotebookApp.token=''", "--NotebookApp.password=''", \
     "--ServerApp.disable_check_xsrf=True"]