package io.github.openfacade.table.spring.reactive.mysql;

import io.github.openfacade.table.spring.test.common.TestConfig;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

public class ReactiveMysqlTestConfig extends TestConfig {
    @Container
    private static final MySQLContainer<?> mysqlContainer = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    @Bean
    public DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
        return DatabaseClient.create(connectionFactory);
    }

    @Bean
    public ConnectionFactory connectionFactory(R2dbcProperties r2dbcProperties) {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "mysql")
                .option(ConnectionFactoryOptions.HOST, mysqlContainer.getHost())
                .option(ConnectionFactoryOptions.PORT, mysqlContainer.getMappedPort(3306))
                .option(ConnectionFactoryOptions.USER, mysqlContainer.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, mysqlContainer.getPassword())
                .option(ConnectionFactoryOptions.DATABASE, mysqlContainer.getDatabaseName())
                .option(ConnectionFactoryOptions.SSL, false)
                .build();
        return ConnectionFactories.get(options);
    }

    @Bean
    public R2dbcProperties r2dbcProperties() {
        mysqlContainer.start();
        String url = String.format("r2dbc:mysql://%s:%d/%s", mysqlContainer.getHost(),
                mysqlContainer.getMappedPort(3306),
                mysqlContainer.getDatabaseName());
        R2dbcProperties properties = new R2dbcProperties();
        properties.setUrl(url);
        properties.setUsername(mysqlContainer.getUsername());
        properties.setPassword(mysqlContainer.getPassword());
        return properties;
    }
}
