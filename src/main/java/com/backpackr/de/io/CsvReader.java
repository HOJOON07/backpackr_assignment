package com.backpackr.de.io;

import com.backpackr.de.config.JobConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class CsvReader {

    public static final String RAW_VIEW = "raw_events";

    private static final StructType RAW_SCHEMA = new StructType()
            .add("event_time", DataTypes.StringType, true)
            .add("event_type", DataTypes.StringType, true)
            .add("product_id", DataTypes.StringType, true)
            .add("category_id", DataTypes.StringType, true)
            .add("category_code", DataTypes.StringType, true)
            .add("brand", DataTypes.StringType, true)
            .add("price", DataTypes.StringType, true)
            .add("user_id", DataTypes.StringType, true)
            .add("user_session", DataTypes.StringType, true);

    private final SparkSession spark;
    private final JobConfig config;

    public CsvReader(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public Dataset<Row> read() {
        return spark.read()
                .option("header", String.valueOf(config.csvHeader()))
                .option("inferSchema", "false")
                .option("enforceSchema", "true")
                .option("mode", "PERMISSIVE")
                .schema(RAW_SCHEMA)
                .csv(config.inputPath());
    }

    public Dataset<Row> readAndRegister() {
        Dataset<Row> raw = read();
        raw.createOrReplaceTempView(RAW_VIEW);
        return raw;
    }
}
