package com.backpackr.de.job;

import com.backpackr.de.config.JobConfig;
import com.backpackr.de.meta.MetastoreManager;
import com.backpackr.de.util.SqlLoader;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public final class BuildWauJob {

    private final SparkSession spark;
    private final JobConfig config;

    public BuildWauJob(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public void run() {
        MetastoreManager metastore = new MetastoreManager(spark, config);
        metastore.createDatabase();
        metastore.createExternalTable();
        metastore.repairPartitions();
        metastore.createWauTable();

        spark.sql(SqlLoader.loadAndBind("07_build_wau.sql", Map.of(
                "database", config.database(),
                "table", config.table(),
                "wau_table", config.wauTable())));

        System.out.println("=== Gold: " + config.database() + "." + config.wauTable() + " ===");
        spark.sql("SELECT * FROM " + config.database() + "." + config.wauTable()
                + " ORDER BY week_start_date").show(200, false);
    }
}
