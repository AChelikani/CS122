package edu.caltech.nanodb.expressions;


import java.util.HashMap;

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

    /** A logging object for reporting anything interesting that happens. **/
    //private static Logger logger = Logger.getLogger(GroupAggregationExpressionProcessor.class);

    private HashMap<String, FunctionCall> aggregateMap;
    private int counter;

    public GroupAggregationProcessor() {
        aggregateMap = new HashMap();
        counter = 1;
    }


    /**
     * DOCUMENTATION NEEDED
     *
     * @param e the expression node being entered
     */
    public void enter(Expression e) {
        return;
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
                String name = AGGREGATE_PREFIX + counter;
                aggregateMap.put(name, call);
                counter += 1;

                ColumnName replacementColumn = new ColumnName(name);
                ColumnValue newExpr = new ColumnValue(replacementColumn);
                return newExpr;
            }
        }
        return e;
    }

    public HashMap<String, FunctionCall> getAggregateMap() {
        return aggregateMap;
    }

}
