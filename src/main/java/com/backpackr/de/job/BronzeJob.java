package com.backpackr.de.job;

import static org.apache.spark.sql.functions.expr;

import com.backpackr.de.config.JobConfig;
import com.backpackr.de.io.CsvReader;
import com.backpackr.de.io.PartitionedWriter;
import com.backpackr.de.meta.MetastoreManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class BronzeJob {

    private static final String PARTITION_COLUMN = "event_date_utc";

    private final SparkSession spark;
    private final JobConfig config;

    public BronzeJob(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public void run() {
        Dataset<Row> raw = new CsvReader(spark, config).read();
        Dataset<Row> bronze = raw.withColumn(PARTITION_COLUMN,
                expr("to_date(try_to_timestamp(regexp_replace(event_time, ' UTC$', '')))"));

        new PartitionedWriter(config.bronzeOutputPath(), PARTITION_COLUMN).write(bronze);

        MetastoreManager metastore = new MetastoreManager(spark, config);
        metastore.createDatabase();
        metastore.createBronzeTable();
        metastore.repairBronzePartitions();
    }
}
