package com.eventfanout.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DbInitTest {

    @TempDir Path temp;

    @Test
    void createsTablesAndBatchDir() throws Exception {
        Path batches = temp.resolve("batches");
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setUrl("jdbc:sqlite:" + temp.resolve("init.db").toAbsolutePath());
        JdbcTemplate jdbc = new JdbcTemplate(ds);

        new DbInit(jdbc, batches).init();

        assertThat(Files.isDirectory(batches)).isTrue();
        Integer tables = jdbc.queryForObject(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='subscriptions'",
                Integer.class
        );
        assertThat(tables).isEqualTo(1);
    }
}
