package io.github.openfacade.table.spring.reactive.mysql;

import io.github.openfacade.table.reactive.api.ReactiveTableOperations;
import io.github.openfacade.table.spring.test.common.TestEntity;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;

import java.util.List;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(classes = ReactiveMysqlTestConfig.class)
public class ReactiveMysqlTableOperationsTest {

    @Autowired
    private DatabaseClient databaseClient;

    @Autowired
    private ReactiveTableOperations reactiveTableOperations;

    @BeforeAll
    void beforeAll() {
        String createTableSql = """
                CREATE TABLE IF NOT EXISTS test_entity (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    blob_bytes_field BLOB
                );
                """;
        databaseClient.sql(createTableSql).fetch()
                .rowsUpdated()
                .doOnSuccess(count -> log.info("table created successfully."))
                .doOnError(error -> log.error("error creating table", error))
                .block();
    }

    @Test
    void testInsertFindAllDelete() {
        // Step 1: Insert a TestEntity object
        TestEntity entityToInsert = new TestEntity();
        entityToInsert.setBlobBytesField("Sample Data".getBytes());

        reactiveTableOperations.insert(entityToInsert)
                .doOnSuccess(insertedEntity -> log.info("Inserted entity: {}", insertedEntity))
                .block();

        // Step 2: Find all TestEntity objects and verify the inserted data
        Flux<TestEntity> findAllResult = reactiveTableOperations.findAll(TestEntity.class);

        List<TestEntity> entities = findAllResult
                .doOnNext(entity -> log.info("Retrieved entity: {}", entity))
                .collectList()
                .block();

        Assertions.assertNotNull(entities, "Retrieved entities should not be null");
        Assertions.assertFalse(entities.isEmpty(), "Retrieved entities should not be empty");

        TestEntity retrievedEntity = entities.get(0);
        Assertions.assertNotNull(retrievedEntity.getId(), "ID should not be null after insertion");
        Assertions.assertArrayEquals("Sample Data".getBytes(), retrievedEntity.getBlobBytesField(), "Blob data should match");

        // Step 3: Delete all TestEntity objects
        reactiveTableOperations.deleteAll(TestEntity.class)
                .doOnSuccess(deletedCount -> log.info("Deleted {} entities", deletedCount))
                .block();

        // Step 4: Verify that the table is empty after deletion
        List<TestEntity> emptyResult = reactiveTableOperations.findAll(TestEntity.class)
                .collectList()
                .block();

        Assertions.assertTrue(emptyResult.isEmpty(), "The table should be empty after deletion");
    }
}
