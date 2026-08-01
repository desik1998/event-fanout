package com.eventfanout.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class EventBuffer {

    private static final long MAX_BYTES = 10L * 1024 * 1024;
    private static final int MAX_MESSAGES = 200;
    private static final long FLUSH_AFTER_MS = 200;

    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;
    private final Path batchesDir;
    private final Object lock = new Object();
    private final AtomicBoolean shutdownDumpDone = new AtomicBoolean(false);

    private final List<BufferedEvent> buffer = new ArrayList<>();
    private long bufferBytes;
    private long bufferStartedAtMs;

    @Autowired
    public EventBuffer(ObjectMapper mapper, JdbcTemplate jdbc) {
        this(mapper, jdbc, Path.of("data/batches"));
    }

    public EventBuffer(ObjectMapper mapper, JdbcTemplate jdbc, Path batchesDir) {
        this.mapper = mapper;
        this.jdbc = jdbc;
        this.batchesDir = batchesDir;
    }

    /**
     * Java "kill switch": runs on SIGTERM / SIGINT / normal JVM exit
     * (Spring stop, Ctrl+C, {@code kill <pid>}). Does NOT run on {@code kill -9} or power loss.
     */
    @PostConstruct
    void registerShutdownDump() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::dumpToDiskOnShutdown, "event-buffer-shutdown-hook"));
    }

    /** Spring context closing (graceful app stop). */
    @PreDestroy
    void onSpringShutdown() {
        dumpToDiskOnShutdown();
    }

    /** Flush any in-memory messages to disk before the process dies. */
    public void dumpToDiskOnShutdown() {
        if (!shutdownDumpDone.compareAndSet(false, true)) {
            return;
        }
        int pending;
        synchronized (lock) {
            pending = buffer.size();
        }
        if (pending == 0) {
            return;
        }
        System.err.println("[EventBuffer] shutdown dump: writing " + pending + " buffered event(s) to disk");
        try {
            flush();
            System.err.println("[EventBuffer] shutdown dump: done");
        } catch (Exception ex) {
            System.err.println("[EventBuffer] shutdown dump failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
        }
    }

    public Enqueued enqueue(String customerId, String type, String source, Map<String, Object> payload) {
        String eventId = UUID.randomUUID().toString();
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("id", eventId);
        event.put("customerId", customerId);
        event.put("type", type);
        event.put("source", source);
        event.put("payload", payload == null ? Map.of() : payload);
        event.put("createdAt", Instant.now().toString());

        try {
            byte[] bytes = mapper.writeValueAsBytes(event);
            CompletableFuture<String> future = new CompletableFuture<>();
            boolean flushNow;
            synchronized (lock) {
                if (buffer.isEmpty()) {
                    bufferStartedAtMs = System.currentTimeMillis();
                }
                buffer.add(new BufferedEvent(eventId, bytes, future));
                bufferBytes += bytes.length + 1L;
                flushNow = bufferBytes >= MAX_BYTES || buffer.size() >= MAX_MESSAGES;
            }
            if (flushNow) {
                flush();
            }
            return new Enqueued(eventId, future);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to serialize event: " + e.getMessage(), e);
        }
    }

    @Scheduled(fixedDelay = 200)
    public void tick() {
        synchronized (lock) {
            if (buffer.isEmpty()) {
                return;
            }
            if (System.currentTimeMillis() - bufferStartedAtMs < FLUSH_AFTER_MS) {
                return;
            }
        }
        flush();
    }

    public void flush() {
        List<BufferedEvent> toFlush;
        synchronized (lock) {
            if (buffer.isEmpty()) {
                return;
            }
            toFlush = new ArrayList<>(buffer);
            buffer.clear();
            bufferBytes = 0;
            bufferStartedAtMs = 0;
        }

        String batchId = UUID.randomUUID().toString();
        Path path = batchesDir.resolve(batchId + ".jsonl");
        try {
            // Disk first — survives even if DB insert fails during shutdown.
            writeFile(path, toFlush);
            try {
                jdbc.update(
                        "INSERT INTO batches(id, path, status, created_at, event_count, size_bytes) VALUES (?,?,?,?,?,?)",
                        batchId, path.toString(), "READY", Instant.now().toString(), toFlush.size(), Files.size(path)
                );
            } catch (Exception dbEx) {
                // File is durable; leave a marker so a worker/recovery pass can register it.
                Path marker = path.resolveSibling(path.getFileName() + ".unregistered");
                Files.writeString(marker, dbEx.getMessage() == null ? "db insert failed" : dbEx.getMessage());
                System.err.println("[EventBuffer] batch file written but DB register failed: " + path);
            }
            for (BufferedEvent e : toFlush) {
                e.future.complete(batchId);
            }
        } catch (Exception ex) {
            for (BufferedEvent e : toFlush) {
                e.future.completeExceptionally(ex);
            }
        }
    }

    private void writeFile(Path path, List<BufferedEvent> events) throws Exception {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.createDirectories(path.getParent());
        try (var out = Files.newOutputStream(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (BufferedEvent e : events) {
                out.write(e.jsonBytes);
                out.write('\n');
            }
            out.flush();
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
        }
        Files.move(tmp, path);
    }

    public record Enqueued(String eventId, CompletableFuture<String> future) {
    }

    private record BufferedEvent(String eventId, byte[] jsonBytes, CompletableFuture<String> future) {
    }
}
