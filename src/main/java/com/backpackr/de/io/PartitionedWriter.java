package com.backpackr.de.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public final class PartitionedWriter {

    public static final String PARTITION_COLUMN = "event_date_kst";

    private final String outputPath;

    public PartitionedWriter(String outputPath) {
        this.outputPath = outputPath;
    }

    public void write(Dataset<Row> events) {
        events.write()
                .mode(SaveMode.Overwrite)
                .option("compression", "snappy")
                .partitionBy(PARTITION_COLUMN)
                .parquet(outputPath);
    }
}
