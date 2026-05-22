package com.backpackr.de.util;

import com.backpackr.de.config.JobConfig;
import org.apache.spark.sql.SparkSession;

/**
 * Builds the SparkSession with the settings this job depends on for correctness:
 *
 * <ul>
 *   <li><b>session.timeZone=UTC</b> — the raw event_time is UTC. Pinning the
 *       session zone makes timestamp parsing and {@code unix_timestamp()}
 *       deterministic regardless of the host/container locale. KST is derived
 *       explicitly with {@code from_utc_timestamp} for display/partitioning.</li>
 *   <li><b>partitionOverwriteMode=dynamic</b> — re-runs overwrite only the
 *       partitions present in the output, never sibling partitions. This is what
 *       makes reprocessing idempotent.</li>
 *   <li><b>ansi.enabled=true</b> — kept on (Spark 4 default) so logic errors fail
 *       fast; CSV parsing uses try_* functions to tolerate dirty input rows.</li>
 * </ul>
 */
public final class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession create(JobConfig config, String appName) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.warehouse.dir", config.warehousePath());

        String master = config.sparkMaster();
        if (master != null && !master.isBlank()) {
            builder.master(master);
        }

        if ("remote".equalsIgnoreCase(config.metastoreMode())
                && !config.hiveMetastoreUris().isBlank()) {
            builder.config("hive.metastore.uris", config.hiveMetastoreUris());
        }

        return builder.enableHiveSupport().getOrCreate();
    }
}
