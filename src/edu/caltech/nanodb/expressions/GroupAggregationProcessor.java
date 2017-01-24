package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.queryast.SelectValue;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

/**
 * <p>
 *     DOCUMENTATION NEEDED
 * </p>
 */
public class GroupAggregationProcessor implements ExpressionProcessor {

    /** Prefixes for auto-generated column names. */
    private static final String AGGREGATE_PREFIX = "#A";
    private static final String GROUP_BY_PREFIX = "#G";

    /** Maps from auto-generated column names to function calls or complex expressions. */
    private HashMap<String, FunctionCall> aggregateMap;
    private HashMap<String, Expression> groupByMap;

    /**
     * Set to true when enter() is called on an aggregate expression, and
     * set to false when leave() is called on an aggregate expression. Used
     * to determine whether a aggregate expression is contained within another.
     */
    private boolean hasAggregateParent;

    /**
     * Counter for the number of aggregate functions found. Used to create auto-generated
     * function names.
     */
    private int aggregateCounter;

    public GroupAggregationProcessor() {
        aggregateMap = new HashMap<>();
        groupByMap = new HashMap<>();
        hasAggregateParent = false;
        aggregateCounter = 0;
    }


    /**
     * If the given expression is an aggregate function call, this method
     * checks if hasAggregateParent is true. If true, it throws an
     * {@code IllegalArgumentException}. Otherwise, hasAggregateParent
     * is set to true.
     *
     * @param e the {@code Expression} node being entered
     */
    public void enter(Expression e) throws IllegalArgumentException {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (hasAggregateParent) {
                    throw new IllegalArgumentException("Cannot have aggregate function inside " +
                            "another aggregate function");
                } else {
                    hasAggregateParent = true;
                }
            }
        }
    }

    /**
     * If the given expression is an aggregate function call, the method
     * maps an auto-generated name to the function call in aggregateMap,
     * and creates a new {@code ColumnValue} expression to replace it. If
     * the given expression is a complex expression contained the groupByMap,
     * it also replaces it with a {@code ColumnValue} expression.
     *
     * @param e the expression node being left
     *
     * @return if {@code e} was an aggregate function called or contained in
     *         groupByMap, the new expression created by the processor. Otherwise,
     *         simply returns the given {@code e}.
     */
    public Expression leave(Expression e) {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                hasAggregateParent = false;

                // Map auto-generated name to function call
                String name = AGGREGATE_PREFIX + aggregateCounter;
                aggregateMap.put(name, call);
                aggregateCounter += 1;

                // Create replacement ColumnValue expression
                ColumnName replacementColumn = new ColumnName(name);
                ColumnValue newExpr = new ColumnValue(replacementColumn);
                return newExpr;
            }
        }
        // Check if expression is contained in groupByMap
        else if (!(e instanceof ColumnValue)) {
            for (String key : groupByMap.keySet()) {
                if (e.equals(groupByMap.get(key))) {
                    // Create replacement ColumnValue expression
                    ColumnName replacementColumn = new ColumnName(key);
                    ColumnValue newExpr = new ColumnValue(replacementColumn);
                    return newExpr;
                }
            }
        }
        return e;
    }

    /**
     * Scans for complex expressions in the given <tt>GROUP BY</tt> expressions,
     * and maps auto-generated column-names to them.
     *
     * @param groupByExprs the <tt>GROUP BY</tt> expressions to scan
     *
     * @return list of {@code SelectValue} objects to be used in a {@code ProjectNode}
     *         to create simple columns containing the complex expressions found.
     */
    public List<SelectValue> processGroupByExprs(List<Expression> groupByExprs) {
        List<SelectValue> selectValues = new ArrayList<>();
        int groupByCounter = 0;

        for (int i = 0; i < groupByExprs.size(); i++) {
            Expression e = groupByExprs.get(i);

            if (e instanceof ColumnValue) {
                continue;
            }

            // Map auto-generated name to expression
            String name = GROUP_BY_PREFIX + groupByCounter;
            groupByMap.put(name, e);
            selectValues.add(new SelectValue(e, name));
            groupByCounter += 1;

            // Create replacement ColumnValue expression
            ColumnName replacementColumn = new ColumnName(name);
            groupByExprs.set(i, new ColumnValue(replacementColumn));
        }

        return selectValues;
    }

    /**
     * @return the mapping from auto-generated names to aggregation function calls
     */
    public Map<String, FunctionCall> getAggregateMap() {
        return aggregateMap;
    }

    /**
     * Scans the {@code SelectValue} objects for auto-generated names created by
     * the processor, and replaces them with a descriptive name, e.g. #A0 ->
     * SUM(C).
     *
     * @param selectValues the {@code SelectValue} objects to scan
     */
    public void renameSelectValues(List<SelectValue> selectValues) {
        for (int i=0; i < selectValues.size(); i++) {
            SelectValue sv = selectValues.get(i);
            if (sv.getAlias() == null) {
                String columnName = sv.toString();

                for (String key : aggregateMap.keySet()) {
                    columnName = columnName.replaceAll(key, aggregateMap.get(key).toString());
                }

                for (String key : groupByMap.keySet()) {
                    columnName = columnName.replaceAll(key, groupByMap.get(key).toString());
                }

                // Replace SelectValue if column name changed
                if (!columnName.equals(sv.toString())) {
                    selectValues.set(i, new SelectValue(sv.getExpression(), columnName));
                }
            }
        }
    }

}
