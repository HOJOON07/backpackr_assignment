package com.backpackr.de.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class SqlLoader {

    private static final String SQL_DIR = "sql/";

    private SqlLoader() {
    }

    public static String load(String fileName) {
        String resource = SQL_DIR + fileName;
        try (InputStream in = SqlLoader.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("SQL resource not found: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read SQL resource: " + resource, e);
        }
    }

    public static String loadAndBind(String fileName, Map<String, String> params) {
        String sql = load(fileName);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            sql = sql.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        if (sql.contains("${")) {
            throw new IllegalStateException(
                    "Unbound placeholder remains in " + fileName + " after binding " + params.keySet());
        }
        return sql;
    }
}
