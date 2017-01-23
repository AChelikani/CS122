package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.queryast.SelectValue;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

/**
 * <p>
 *     DOCUMENTATION NEEDED
 * </p>
 */
public class GroupAggregationProcessor implements ExpressionProcessor {

    private static final String AGGREGATE_PREFIX = "#A";
    private static final String GROUP_BY_PREFIX = "#G";

    /** A logging object for reporting anything interesting that happens. **/
    //private static Logger logger = Logger.getLogger(GroupAggregationExpressionProcessor.class);

    private HashMap<String, FunctionCall> aggregateMap;
    private HashMap<String, Expression> groupByMap;
    private boolean hasAggregateParent;
    private int counter;

    public GroupAggregationProcessor() {
        aggregateMap = new HashMap<>();
        groupByMap = new HashMap<>();
        hasAggregateParent = false;
        counter = 0;
    }


    /**
     * DOCUMENTATION NEEDED
     *
     * @param e the expression node being entered
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
     * DOC NEEDED
     *
     * @param e the expression node being left
     *
     * @return DOCUMENTATION NEEDED
     */
    public Expression leave(Expression e) {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                hasAggregateParent = false;

                String name = AGGREGATE_PREFIX + counter;
                aggregateMap.put(name, call);
                counter += 1;

                ColumnName replacementColumn = new ColumnName(name);
                ColumnValue newExpr = new ColumnValue(replacementColumn);
                return newExpr;
            }
        }
        else if (!(e instanceof ColumnValue)) {
            for (String key : groupByMap.keySet()) {
                if (e.equals(groupByMap.get(key))) {
                    ColumnName replacementColumn = new ColumnName(key);
                    ColumnValue newExpr = new ColumnValue(replacementColumn);
                    return newExpr;
                }
            }
        }
        return e;
    }

    public List<SelectValue> processGroupByExprs(List<Expression> groupByExprs) {
        List<SelectValue> selectValues = new ArrayList<>();
        int groupByCounter = 0;

        for (int i = 0; i < groupByExprs.size(); i++) {
            Expression e = groupByExprs.get(i);

            if (e instanceof ColumnValue) {
                continue;
            }

            String name = GROUP_BY_PREFIX + groupByCounter;
            groupByMap.put(name, e);
            selectValues.add(new SelectValue(e, name));
            groupByCounter += 1;

            ColumnName replacementColumn = new ColumnName(name);
            groupByExprs.set(i, new ColumnValue(replacementColumn));
        }

        return selectValues;
    }

    public Map<String, FunctionCall> getAggregateMap() {
        return aggregateMap;
    }

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

                if (!columnName.equals(sv.toString())) {
                    selectValues.set(i, new SelectValue(sv.getExpression(), columnName));
                }
            }
        }
    }

}
