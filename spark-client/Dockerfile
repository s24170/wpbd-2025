FROM spark:4.0.0-scala2.13-java21-python3-ubuntu

WORKDIR /app

USER root

RUN pip install --no-cache-dir -v minio
RUN #pip install --no-cache-dir delta-spark==4.0.0

USER spark

COPY src/ /app/src/
