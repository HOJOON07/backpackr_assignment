package com.backpackr.de.util;

import com.backpackr.de.config.JobConfig;
import org.apache.spark.sql.SparkSession;

public final class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession create(JobConfig config, String appName) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.warehouse.dir", config.warehousePath());

        String master = config.sparkMaster();
        if (master != null && !master.isBlank()) {
            builder.master(master);
        }

        if ("remote".equalsIgnoreCase(config.metastoreMode())
                && !config.hiveMetastoreUris().isBlank()) {
            builder.config("hive.metastore.uris", config.hiveMetastoreUris());
        } else {
            builder.config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    "jdbc:derby:;databaseName=" + config.metastoreDbPath() + ";create=true");
        }

        return builder.enableHiveSupport().getOrCreate();
    }
}
