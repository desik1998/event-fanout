package com.eventfanout.support;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;

/** Shared SQLite + schema helper for unit tests. */
public final class TestDb {

    private TestDb() {
    }

    public static JdbcTemplate create(Path dbFile) throws Exception {
        Files.createDirectories(dbFile.getParent());
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setUrl("jdbc:sqlite:" + dbFile.toAbsolutePath());
        JdbcTemplate jdbc = new JdbcTemplate(ds);
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
        return jdbc;
    }
}
