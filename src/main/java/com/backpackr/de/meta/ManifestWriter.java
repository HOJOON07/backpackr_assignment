package com.backpackr.de.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class ManifestWriter {

    private final String manifestDir;
    private final Configuration hadoopConf;
    private final ObjectMapper mapper;

    public ManifestWriter(String manifestDir, Configuration hadoopConf) {
        this.manifestDir = manifestDir;
        this.hadoopConf = hadoopConf;
        this.mapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void write(AuditManifest manifest) {
        Path dir = new Path(manifestDir);
        Path finalPath = new Path(dir, "run_id=" + manifest.runId() + ".json");
        Path tmpPath = new Path(dir, "run_id=" + manifest.runId() + ".json.tmp");
        try {
            byte[] json = mapper.writeValueAsBytes(manifest);
            FileSystem fs = FileSystem.get(URI.create(manifestDir), hadoopConf);
            fs.mkdirs(dir);
            try (FSDataOutputStream out = fs.create(tmpPath, true)) {
                out.write(json);
            }
            fs.delete(finalPath, false);
            if (!fs.rename(tmpPath, finalPath)) {
                throw new IOException("Failed to rename " + tmpPath + " to " + finalPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write manifest: " + finalPath, e);
        }
    }
}
