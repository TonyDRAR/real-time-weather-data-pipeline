FROM apache/spark:3.5.3

USER root

RUN mkdir -p \
    /opt/pipeline/jobs \
    /opt/pipeline/data/bronze \
    /opt/pipeline/data/silver \
    /opt/pipeline/data/checkpoints

COPY jobs/weather_stream.py /opt/pipeline/jobs/weather_stream.py

WORKDIR /opt/pipeline

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.5", \
     "--conf", "spark.sql.shuffle.partitions=3", \
     "--conf", "spark.sql.session.timeZone=UTC", \
     "--conf", "spark.ui.port=4040", \
     "/opt/pipeline/jobs/weather_stream.py"]
