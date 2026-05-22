#!/usr/bin/env bash
#
# Thin wrapper around spark-submit. All application arguments are passed through
# from the container command, e.g.:
#   docker compose run --rm spark-job --mode ingest --start-date 2019-10-01 --end-date 2019-11-30
#
set -euo pipefail

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
APP_JAR="/opt/app/app.jar"
MAIN_CLASS="com.backpackr.de.App"

# Local single-JVM execution. Driver memory is generous because the full dataset
# (~110M rows across Oct+Nov) is sessionized in-process.
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
MASTER="${SPARK_MASTER:-local[*]}"

exec "${SPARK_SUBMIT}" \
  --master "${MASTER}" \
  --class "${MAIN_CLASS}" \
  --conf "spark.driver.memory=${DRIVER_MEMORY}" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
  "${APP_JAR}" "$@"
