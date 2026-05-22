package com.backpackr.de.job;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import com.backpackr.de.config.JobConfig;
import com.backpackr.de.io.CsvReader;
import com.backpackr.de.io.PartitionedWriter;
import com.backpackr.de.meta.AuditManifest;
import com.backpackr.de.meta.ManifestWriter;
import com.backpackr.de.meta.MetastoreManager;
import com.backpackr.de.util.SqlLoader;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class IngestionJob {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter RUN_ID_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final SparkSession spark;
    private final JobConfig config;

    public IngestionJob(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public void run(LocalDate startDate, LocalDate endDate) {
        String runId = ZonedDateTime.now(KST).format(RUN_ID_FORMAT);
        ManifestWriter manifestWriter = new ManifestWriter(
                config.manifestPath(), spark.sparkContext().hadoopConfiguration());
        AuditManifest manifest = AuditManifest.running(
                runId, config.inputPath(), config.outputPath(),
                startDate.toString(), endDate.toString(), nowIso());
        manifestWriter.write(manifest);

        try {
            LocalDate readStartUtc = startDate.minusDays(config.sessionBufferDays());
            spark.table(config.database() + "." + config.bronzeTable())
                    .filter(col("event_date_utc").geq(to_date(lit(readStartUtc.toString())))
                            .and(col("event_date_utc").leq(to_date(lit(endDate.toString())))))
                    .createOrReplaceTempView(CsvReader.RAW_VIEW);

            String sql = SqlLoader.loadAndBind("02_sessionize_events.sql",
                    Map.of("gap_seconds", String.valueOf(config.sessionGapSeconds())));
            Dataset<Row> sessionized = spark.sql(sql);

            Dataset<Row> target = sessionized.filter(
                            col("event_date_kst").geq(to_date(lit(startDate.toString())))
                                    .and(col("event_date_kst").leq(to_date(lit(endDate.toString())))))
                    .cache();
            long rowCount = target.count();

            new PartitionedWriter(config.outputPath(), "event_date_kst").write(target);

            MetastoreManager metastore = new MetastoreManager(spark, config);
            metastore.createDatabase();
            metastore.createExternalTable();
            metastore.repairPartitions();

            manifestWriter.write(manifest.succeeded(rowCount, nowIso()));
        } catch (Exception e) {
            manifestWriter.write(manifest.failed(String.valueOf(e.getMessage()), nowIso()));
            throw e;
        }
    }

    private static String nowIso() {
        return ZonedDateTime.now(KST).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
