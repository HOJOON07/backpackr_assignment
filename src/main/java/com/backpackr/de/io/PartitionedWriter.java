package com.backpackr.de.io;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public final class PartitionedWriter {

    private static final long MAX_RECORDS_PER_FILE = 1_000_000L;

    private final String outputPath;
    private final String partitionColumn;

    public PartitionedWriter(String outputPath, String partitionColumn) {
        this.outputPath = outputPath;
        this.partitionColumn = partitionColumn;
    }

    public void write(Dataset<Row> events) {
        events.repartition(col(partitionColumn))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "snappy")
                .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
                .partitionBy(partitionColumn)
                .parquet(outputPath);
    }
}
