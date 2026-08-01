package com.eventfanout.store;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class DbInit {

    private final JdbcTemplate jdbc;
    private final Path batchesDir;

    @Autowired
    public DbInit(JdbcTemplate jdbc, Path batchesDir) {
        this.jdbc = jdbc;
        this.batchesDir = batchesDir;
    }

    public DbInit(JdbcTemplate jdbc) {
        this(jdbc, Path.of("data/batches"));
    }

    @PostConstruct
    void init() throws Exception {
        Files.createDirectories(batchesDir);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS batches (
              id TEXT PRIMARY KEY,
              path TEXT NOT NULL,
              status TEXT NOT NULL,
              owner_host TEXT,
              started_at TEXT,
              created_at TEXT NOT NULL,
              completed_at TEXT,
              event_count INTEGER NOT NULL DEFAULT 0,
              size_bytes INTEGER NOT NULL DEFAULT 0
            )
            """);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
              id TEXT PRIMARY KEY,
              customer_id TEXT NOT NULL,
              url TEXT NOT NULL,
              filter_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """);
        // Migrate older DBs created without customer_id
        try {
            jdbc.execute("ALTER TABLE subscriptions ADD COLUMN customer_id TEXT NOT NULL DEFAULT 'default'");
        } catch (Exception ignored) {
            // column already exists
        }
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS deliveries (
              id TEXT PRIMARY KEY,
              batch_id TEXT NOT NULL,
              event_id TEXT NOT NULL,
              subscription_id TEXT NOT NULL,
              status TEXT NOT NULL,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              next_attempt_at TEXT,
              webhook_url TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS delivery_attempts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              delivery_id TEXT NOT NULL,
              attempted_at TEXT NOT NULL,
              http_status INTEGER,
              error TEXT
            )
            """);
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status, started_at)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_customer ON subscriptions(customer_id)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries(event_id)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_sub ON deliveries(subscription_id)");
    }
}
