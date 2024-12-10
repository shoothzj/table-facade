package io.github.openfacade.table.spring.core;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;

@Getter
@AllArgsConstructor
public class TableMetadata {
    private final String tableName;

    private final LinkedHashMap<String, Method> setterMap;

    private final LinkedHashMap<String, Method> getterMap;
}
