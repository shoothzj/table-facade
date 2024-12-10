/*
 * Copyright 2024 OpenFacade Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.openfacade.table.spring.reactive.mysql;

import io.github.openfacade.table.api.anno.Column;
import io.github.openfacade.table.api.anno.Table;
import io.github.openfacade.table.reactive.api.ReactiveTableOperations;
import io.github.openfacade.table.spring.core.TableMetadata;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ReactiveMysqlTableOperations implements ReactiveTableOperations {
    private final Map<Class<?>, TableMetadata> classMap = new ConcurrentHashMap<>();

    private final DatabaseClient databaseClient;

    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z0-9_]+$");

    @Override
    public <T> Mono<T> insert(T object) {
        Class<?> type = object.getClass();
        classMap.putIfAbsent(type, parseClass(type));
        TableMetadata metadata = classMap.get(type);

        String tableName = escapeIdentifier(metadata.getTableName());
        List<String> columns = metadata.getSetterMap().keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.toList());
        String placeholders = columns.stream().map(col -> "?").collect(Collectors.joining(", "));

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, String.join(", ", columns), placeholders);

        Map<String, Object> parameters = metadata.getGetterMap().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                return entry.getValue().invoke(object);
                            } catch (Exception e) {
                                throw new RuntimeException("Error invoking getter", e);
                            }
                        }
                ));

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(query);
        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            spec = spec.bind(param.getKey(), Parameter.fromOrEmpty(param.getValue(), param.getValue() != null ? param.getValue().getClass() : Object.class));
        }

        return spec.fetch().rowsUpdated().thenReturn(object);
    }

    @Override
    public <T> Flux<T> findAll(Class<T> type) {
        classMap.putIfAbsent(type, parseClass(type));
        TableMetadata metadata = classMap.get(type);

        String tableName = escapeIdentifier(metadata.getTableName());
        List<String> escapedColumns = metadata.getSetterMap().keySet().stream()
                .map(this::escapeIdentifier)
                .collect(Collectors.toList());

        String query = "SELECT " + String.join(", ", escapedColumns) + " FROM " + tableName;

        return databaseClient.sql(query)
                .map((row, metadataAccessor) -> mapRowToEntity(row, type, metadata))
                .all();
    }

    @Override
    public <T> Mono<Long> deleteAll(Class<T> type) {
        classMap.putIfAbsent(type, parseClass(type));
        TableMetadata metadata = classMap.get(type);

        String tableName = escapeIdentifier(metadata.getTableName());
        String query = "DELETE FROM " + tableName;

        return databaseClient.sql(query)
                .fetch()
                .rowsUpdated()
                .map(Long::valueOf);
    }

    private TableMetadata parseClass(Class<?> type) {
        if (!type.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("Class " + type.getName() + " is missing @Table annotation");
        }

        String tableName = type.getAnnotation(Table.class).name();

        LinkedHashMap<String, Method> setterMap = new LinkedHashMap<>();
        LinkedHashMap<String, Method> getterMap = new LinkedHashMap<>();

        for (var field : type.getDeclaredFields()) {
            if (field.isAnnotationPresent(Column.class)) {
                String columnName = field.getAnnotation(Column.class).name();
                String fieldName = field.getName();
                String capitalizedFieldName = capitalize(fieldName);

                try {
                    // Find the getter method
                    Method getter = type.getDeclaredMethod("get" + capitalizedFieldName);
                    getterMap.put(columnName, getter);

                    // Find the setter method
                    Method setter = type.getDeclaredMethod("set" + capitalizedFieldName, field.getType());
                    setterMap.put(columnName, setter);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Getter or setter not found for field: " + fieldName, e);
                }
            }
        }

        return new TableMetadata(tableName, setterMap, getterMap);
    }

    private <T> T mapRowToEntity(io.r2dbc.spi.Row row, Class<T> type, TableMetadata metadata) {
        try {
            T instance = type.getDeclaredConstructor().newInstance();

            for (Map.Entry<String, Method> entry : metadata.getSetterMap().entrySet()) {
                String columnName = entry.getKey();
                Method setter = entry.getValue();
                Class<?> parameterType = setter.getParameterTypes()[0];

                setter.invoke(instance, row.get(columnName, parameterType));
            }

            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error mapping row to entity: " + type.getName(), e);
        }
    }

    private String escapeIdentifier(String identifier) {
        if (identifier == null || !VALID_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
        return "`" + identifier + "`";
    }

    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
