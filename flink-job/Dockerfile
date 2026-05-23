FROM apache/flink:1.16.1-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean

WORKDIR /opt/flink-job

COPY flink-job/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY flink-job/ .

CMD ["python", "main.py"]