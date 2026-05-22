package com.backpackr.de.job;

import com.backpackr.de.config.JobConfig;
import com.backpackr.de.meta.MetastoreManager;
import com.backpackr.de.util.SqlLoader;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public final class WauJob {

    private final SparkSession spark;
    private final JobConfig config;

    public WauJob(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public void run() {
        MetastoreManager metastore = new MetastoreManager(spark, config);
        metastore.createDatabase();
        metastore.createExternalTable();
        metastore.repairPartitions();

        Map<String, String> params = Map.of(
                "database", config.database(),
                "table", config.table());

        System.out.println("=== WAU by user_id ===");
        spark.sql(SqlLoader.loadAndBind("05_wau_by_user.sql", params)).show(200, false);

        System.out.println("=== WAU by session_id ===");
        spark.sql(SqlLoader.loadAndBind("06_wau_by_session.sql", params)).show(200, false);
    }
}
