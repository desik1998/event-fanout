package com.eventfanout.ingest;

import com.eventfanout.support.TestDb;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class EventBufferTest {

    @TempDir Path temp;
    Path batchesDir;
    EventBuffer buffer;
    JdbcTemplate jdbc;

    @BeforeEach
    void setUp() throws Exception {
        batchesDir = temp.resolve("batches");
        Files.createDirectories(batchesDir);
        jdbc = TestDb.create(temp.resolve("fanout.db"));
        buffer = new EventBuffer(new ObjectMapper(), jdbc, batchesDir);
    }

    @Test
    void flushWritesFileAndReadyRow() throws Exception {
        var enqueued = buffer.enqueue("acme", "order.created", "billing", Map.of("id", 1));
        buffer.flush();

        String batchId = enqueued.future().get(2, TimeUnit.SECONDS);
        assertThat(batchId).isNotBlank();
        Path batchFile = batchesDir.resolve(batchId + ".jsonl");
        assertThat(batchFile).exists();
        assertThat(Files.readString(batchFile)).contains("order.created");

        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM batches WHERE id = ? AND status = 'READY'",
                Integer.class, batchId
        );
        assertThat(n).isEqualTo(1);
    }

    @Test
    void flushOnMaxMessages() {
        for (int i = 0; i < 200; i++) {
            buffer.enqueue("acme", "t", "s", Map.of("i", i));
        }
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM batches", Integer.class);
        assertThat(n).isGreaterThanOrEqualTo(1);
    }

    @Test
    void dumpToDiskOnShutdownFlushesBuffer() throws Exception {
        var e = buffer.enqueue("acme", "shutdown.test", "src", Map.of());
        buffer.dumpToDiskOnShutdown();
        String batchId = e.future().get(2, TimeUnit.SECONDS);
        assertThat(batchesDir.resolve(batchId + ".jsonl")).exists();
    }

    @Test
    void dumpIsIdempotent() {
        buffer.dumpToDiskOnShutdown();
        buffer.dumpToDiskOnShutdown();
    }
}
