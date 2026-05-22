package com.backpackr.de.meta;

import com.backpackr.de.config.JobConfig;
import com.backpackr.de.util.SqlLoader;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public final class MetastoreManager {

    private final SparkSession spark;
    private final JobConfig config;

    public MetastoreManager(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public void createDatabase() {
        spark.sql(SqlLoader.loadAndBind("01_create_database.sql",
                Map.of("database", config.database())));
    }

    public void createExternalTable() {
        spark.sql(SqlLoader.loadAndBind("03_create_external_table.sql", Map.of(
                "database", config.database(),
                "table", config.table(),
                "output_path", config.outputPath())));
    }

    public void repairPartitions() {
        spark.sql(SqlLoader.loadAndBind("04_repair_table.sql", Map.of(
                "database", config.database(),
                "table", config.table())));
    }

    public void createWauTable() {
        spark.sql(SqlLoader.loadAndBind("08_create_wau_table.sql", Map.of(
                "database", config.database(),
                "wau_table", config.wauTable(),
                "wau_output_path", config.wauOutputPath())));
    }

    public void createBronzeTable() {
        spark.sql(SqlLoader.loadAndBind("09_create_bronze_table.sql", Map.of(
                "database", config.database(),
                "bronze_table", config.bronzeTable(),
                "bronze_output_path", config.bronzeOutputPath())));
    }

    public void repairBronzePartitions() {
        spark.sql("MSCK REPAIR TABLE " + config.database() + "." + config.bronzeTable());
    }
}
