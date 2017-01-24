package edu.caltech.nanodb.queryeval;


import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.expressions.GroupAggregationProcessor;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class contains implementation details that are common across all query
 * planners.  Planners are of course free to implement these operations
 * separately, but just about all planners have some common functionality, and
 * it's very helpful to implement that functionality once in an abstract base
 * class.
 */
public abstract class AbstractPlannerImpl implements Planner {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(AbstractPlannerImpl.class);


    /** The storage manager used during query planning. */
    protected StorageManager storageManager;


    /** Sets the storage manager to be used during query planning. */
    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Scans the <tt>SELECT</tt> and <tt>HAVING</tt> clauses for aggregate functions, and
     * sets a new <tt>HAVING</tt> expression using the new auto-generated column names. Each
     * {@link edu.caltech.nanodb.queryast.SelectValue} in the <tt>SELECT</tt> clause is changed
     * to reflect the new column names. Mappings from auto-generated column names to aggregate
     * function calls are stored in the processor.
     *
     * @param selClause the <tt>SELECT</tt> clause to scan, which contains the <tt>HAVING</tt>
     *                  clause
     * @param processor the {@link edu.caltech.nanodb.expressions.GroupAggregationProcessor} used
     *                  to process selClause
     */
    public void processAggregateFunctions(SelectClause selClause,
                                          GroupAggregationProcessor processor) {
        scanSelectClauseAggregate(selClause, processor);
        Expression newHavingExpr = scanHavingExprAggregate(selClause.getHavingExpr(), processor);
        selClause.setHavingExpr(newHavingExpr);
    }

    /**
     * Scans the <tt>SELECT</tt> clause for aggregate functions, and changes the expression
     * in each {@link edu.caltech.nanodb.queryast.SelectValue} to reflect the new auto-generated
     * column names. Mappings from auto-generated column names to aggregate
     * function calls are stored in the processor.
     *
     * @param selClause the <tt>SELECT</tt> clause to scan
     * @param processor the {@link edu.caltech.nanodb.expressions.GroupAggregationProcessor} used
     *                  to process selClause and contain the mappings from auto-generated names
     *                  to aggregate function calls.
     */
    public void scanSelectClauseAggregate(SelectClause selClause,
                                          GroupAggregationProcessor processor) {
        List<SelectValue> selectValues = selClause.getSelectValues();
        for (SelectValue sv : selectValues) {
            if (!sv.isExpression()) {
                continue;
            }
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }
    }

    /**
     * Scan the <tt>HAVING</tt> clause for aggregate functions and creates a new <tt>HAVING</tt>
     * expression reflecting the new auto-generated column names. Mappings from auto-generated
     * column names to aggregate function calls are stored in the processor.
     *
     * @param havingExpr the <tt>HAVING</tt> clause to scan
     * @param processor the {@link edu.caltech.nanodb.expressions.GroupAggregationProcessor} used
     *                  to process selClause and contain the mappings from auto-generated names
     *                  to aggregate function calls.
     * @return new <tt>HAVING</tt> expression reflecting the new auto-generated column names
     */
    public Expression scanHavingExprAggregate(Expression havingExpr,
                                              GroupAggregationProcessor processor) {
        Expression newHavingExpr = null;
        if (havingExpr != null) {
            newHavingExpr = havingExpr.traverse(processor);
        }
        return newHavingExpr;
    }

    /**
     * Returns true if the given expression contains any aggregate functions.
     *
     * @param e the expression to check
     *
     * @return true if the given expression contains any aggregate functions, or
     *         false otherwise.
     */
    public boolean containsAggregateFunction(Expression e) {
        GroupAggregationProcessor processor = new GroupAggregationProcessor();
        e.traverse(processor);
        return !processor.getAggregateMap().isEmpty();
    }
}
