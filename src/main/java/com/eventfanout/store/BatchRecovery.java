package com.eventfanout.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

/** On startup, register orphan batch files that never got a DB row. */
@Component
@DependsOn("dbInit")
public class BatchRecovery {

    private final JdbcTemplate jdbc;
    private final Path batchesDir;

    @Autowired
    public BatchRecovery(JdbcTemplate jdbc) {
        this(jdbc, Path.of("data/batches"));
    }

    public BatchRecovery(JdbcTemplate jdbc, Path batchesDir) {
        this.jdbc = jdbc;
        this.batchesDir = batchesDir;
    }

    @PostConstruct
    void recover() throws Exception {
        if (!Files.isDirectory(batchesDir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchesDir, "*.jsonl")) {
            for (Path file : stream) {
                String name = file.getFileName().toString();
                String batchId = name.substring(0, name.length() - ".jsonl".length());
                Integer count = jdbc.queryForObject(
                        "SELECT COUNT(*) FROM batches WHERE id = ?", Integer.class, batchId
                );
                if (count != null && count > 0) {
                    Path marker = file.resolveSibling(name + ".unregistered");
                    if (Files.exists(marker)) {
                        Files.deleteIfExists(marker);
                    }
                    continue;
                }
                long lines;
                try (Stream<String> s = Files.lines(file)) {
                    lines = s.filter(l -> !l.isBlank()).count();
                }
                jdbc.update(
                        "INSERT INTO batches(id, path, status, created_at, event_count, size_bytes) VALUES (?,?,?,?,?,?)",
                        batchId, file.toString(), "READY", Instant.now().toString(), lines, Files.size(file)
                );
                Files.deleteIfExists(file.resolveSibling(name + ".unregistered"));
                System.err.println("[BatchRecovery] registered orphan batch " + batchId);
            }
        }
    }
}
