# Spark image pre-bundling the JDBC + S3 connectors and all pipeline jobs
# so the image is self-contained — works via compose volumes AND when
# Airflow's DockerOperator spawns it without host mounts.
FROM apache/spark:3.5.8-java17-python3

USER root
WORKDIR /opt/spark
ENV SPARK_HOME=/opt/spark

# Needed JARs (downloaded at build time; afterwards the image is self-contained)
RUN curl -sL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -sL -o /opt/spark/jars/postgresql-42.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# Copy all pipeline jobs into the image so the Airflow DockerOperator can
# spawn this image as a standalone container without host volume mounts.
COPY jobs/ /opt/spark/jobs/

USER spark
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]