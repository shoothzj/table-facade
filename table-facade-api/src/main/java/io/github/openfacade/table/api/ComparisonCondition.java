package io.github.openfacade.table.api;

public class ComparisonCondition implements Condition {
    private final String column;

    private final ComparisonOperator operator;

    private final Object value;

    public ComparisonCondition(String column, ComparisonOperator operator, Object value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
}
