package com.backpackr.de.io;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public final class PartitionedWriter {

    public static final String PARTITION_COLUMN = "event_date_kst";
    private static final long MAX_RECORDS_PER_FILE = 1_000_000L;

    private final String outputPath;

    public PartitionedWriter(String outputPath) {
        this.outputPath = outputPath;
    }

    public void write(Dataset<Row> events) {
        events.repartition(col(PARTITION_COLUMN))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "snappy")
                .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
                .partitionBy(PARTITION_COLUMN)
                .parquet(outputPath);
    }
}
