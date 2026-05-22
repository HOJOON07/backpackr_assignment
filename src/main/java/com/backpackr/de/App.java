package com.backpackr.de;

import com.backpackr.de.config.JobConfig;
import com.backpackr.de.job.BuildWauJob;
import com.backpackr.de.job.IngestionJob;
import com.backpackr.de.job.WauJob;
import com.backpackr.de.util.SparkSessionFactory;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public final class App {

    public static void main(String[] args) {
        Map<String, String> opts = parseArgs(args);
        String mode = require(opts, "mode");
        String configPath = require(opts, "config");

        JobConfig config = JobConfig.fromFile(configPath);
        SparkSession spark = SparkSessionFactory.create(config, "BackpackrActivity-" + mode);

        try {
            switch (mode) {
                case "ingest" -> new IngestionJob(spark, config).run(
                        LocalDate.parse(require(opts, "start-date")),
                        LocalDate.parse(require(opts, "end-date")));
                case "wau" -> new WauJob(spark, config).run();
                case "build-wau" -> new BuildWauJob(spark, config).run();
                default -> throw new IllegalArgumentException("Unknown mode: " + mode);
            }
        } finally {
            spark.stop();
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Expected --option, got: " + args[i]);
            }
            m.put(args[i].substring(2), args[i + 1]);
        }
        return m;
    }

    private static String require(Map<String, String> opts, String key) {
        String v = opts.get(key);
        if (v == null) {
            throw new IllegalArgumentException("Missing required argument: --" + key);
        }
        return v;
    }
}
