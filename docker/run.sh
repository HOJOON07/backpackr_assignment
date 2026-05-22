#!/usr/bin/env bash
set -euo pipefail

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
APP_JAR="/opt/app/app.jar"
MAIN_CLASS="com.backpackr.de.App"

DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
MASTER="${SPARK_MASTER:-local[*]}"

exec "${SPARK_SUBMIT}" \
  --master "${MASTER}" \
  --class "${MAIN_CLASS}" \
  --conf "spark.driver.memory=${DRIVER_MEMORY}" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
  "${APP_JAR}" "$@"
