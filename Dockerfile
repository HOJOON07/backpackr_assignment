# syntax=docker/dockerfile:1

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

COPY pom.xml .
RUN mvn -q -e -B dependency:go-offline

COPY src ./src
RUN mvn -q -e -B clean package -DskipTests

FROM apache/spark:4.1.1

USER root
WORKDIR /opt/app

COPY --from=build /build/target/activity-sessionization-1.0.0.jar /opt/app/app.jar
COPY docker/run.sh /opt/app/run.sh
RUN chmod +x /opt/app/run.sh

RUN mkdir -p /opt/app/data /opt/app/spark-warehouse /opt/app/metastore_db \
    && chown -R 185:185 /opt/app

USER 185

ENTRYPOINT ["/opt/app/run.sh"]
