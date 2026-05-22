# syntax=docker/dockerfile:1

# ---- Stage 1: build the application jar with Maven ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Cache dependencies first (pom rarely changes relative to source).
COPY pom.xml .
RUN mvn -q -e -B dependency:go-offline

COPY src ./src
RUN mvn -q -e -B clean package -DskipTests

# ---- Stage 2: Spark runtime ----
FROM apache/spark:4.1.1

USER root
WORKDIR /opt/app

# Application jar (shaded: our code + Jackson, Spark is provided by the image).
COPY --from=build /build/target/activity-sessionization-1.0.0.jar /opt/app/app.jar

# SQL/conf resources are bundled in the jar; we also copy run helper.
COPY docker/run.sh /opt/app/run.sh
RUN chmod +x /opt/app/run.sh

# The spark user (uid 185) must be able to write metastore_db / warehouse.
RUN mkdir -p /opt/app/data /opt/app/spark-warehouse /opt/app/metastore_db \
    && chown -R 185:185 /opt/app

USER 185

ENTRYPOINT ["/opt/app/run.sh"]
