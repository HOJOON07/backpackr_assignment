package com.backpackr.de.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class JobConfig {

    private final Properties props;

    private JobConfig(Properties props) {
        this.props = props;
    }

    public static JobConfig fromFile(String path) {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            p.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load config file: " + path, e);
        }
        return new JobConfig(p);
    }

    private String required(String key) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required config key: " + key);
        }
        return v.trim();
    }

    private String optional(String key, String def) {
        String v = props.getProperty(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    public String env() {
        return optional("env", "local");
    }

    public String sparkMaster() {
        return optional("spark.master", "local[*]");
    }

    public String metastoreMode() {
        return optional("metastore.mode", "local");
    }

    public String hiveMetastoreUris() {
        return optional("hive.metastore.uris", "");
    }

    public String metastoreDbPath() {
        return optional("metastore.db.path", "/opt/app/metastore/metastore_db");
    }

    public String warehousePath() {
        return required("warehouse.path");
    }

    public String inputPath() {
        return required("input.path");
    }

    public String outputPath() {
        return required("output.path");
    }

    public String manifestPath() {
        return required("manifest.path");
    }

    public String database() {
        return required("database");
    }

    public String table() {
        return required("table");
    }

    public String qualifiedTable() {
        return database() + "." + table();
    }

    public String wauTable() {
        return optional("wau.table", "weekly_active_users");
    }

    public String wauOutputPath() {
        return required("wau.output.path");
    }

    public int sessionGapSeconds() {
        return Integer.parseInt(optional("session.gap.seconds", "300"));
    }

    public boolean csvHeader() {
        return Boolean.parseBoolean(optional("csv.header", "true"));
    }
}
