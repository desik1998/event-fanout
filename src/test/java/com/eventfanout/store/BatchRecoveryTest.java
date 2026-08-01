package com.eventfanout.store;

import com.eventfanout.support.TestDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BatchRecoveryTest {

    @TempDir Path temp;
    Path batchesDir;
    JdbcTemplate jdbc;

    @BeforeEach
    void setUp() throws Exception {
        batchesDir = temp.resolve("batches");
        Files.createDirectories(batchesDir);
        jdbc = TestDb.create(temp.resolve("t.db"));
    }

    @Test
    void registersOrphanJsonlAsReady() throws Exception {
        Path orphan = batchesDir.resolve("orphan-1.jsonl");
        Files.writeString(orphan, "{\"id\":\"e1\"}\n{\"id\":\"e2\"}\n");
        Files.writeString(batchesDir.resolve("orphan-1.jsonl.unregistered"), "db down");

        new BatchRecovery(jdbc, batchesDir).recover();

        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM batches WHERE id = 'orphan-1' AND status = 'READY'",
                Integer.class
        );
        assertThat(n).isEqualTo(1);
        assertThat(Files.exists(batchesDir.resolve("orphan-1.jsonl.unregistered"))).isFalse();
    }

    @Test
    void skipsAlreadyRegisteredBatches() throws Exception {
        Path file = batchesDir.resolve("known.jsonl");
        Files.writeString(file, "{}\n");
        jdbc.update(
                "INSERT INTO batches(id, path, status, created_at, event_count, size_bytes) VALUES (?,?,?,?,?,?)",
                "known", file.toString(), "READY", "t", 1, 1
        );

        new BatchRecovery(jdbc, batchesDir).recover();

        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM batches WHERE id = 'known'", Integer.class);
        assertThat(n).isEqualTo(1);
    }
}
