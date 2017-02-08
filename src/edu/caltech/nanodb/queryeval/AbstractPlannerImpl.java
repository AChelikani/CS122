package edu.caltech.nanodb.queryeval;


import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.GroupAggregationProcessor;
import edu.caltech.nanodb.plannodes.HashedGroupAggregateNode;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.plannodes.ProjectNode;
import edu.caltech.nanodb.plannodes.SimpleFilterNode;
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

    /**
     * Processes the given SelectClause for grouping and aggregation.
     * The method will scan the <tt>GROUP BY</tt> clause for complex expressions,
     * and scan the <tt>SELECT</tt> and <tt>HAVING</tt> clauses for aggregate functions.
     * These expressions and functions are stored in the processor
     * and are replaced by auto-generated names.
     *
     * @param plan the child node of the resulting {@link edu.caltech.nanodb.plannodes.PlanNode}
     * @param selClause the <tt>SELECT</tt> clause to process
     *
     * @return the resulting plan node
     */
    public PlanNode processGroupAggregation(PlanNode plan, SelectClause selClause) {
        GroupAggregationProcessor processor = new GroupAggregationProcessor();

        // Scan GROUP BY clause
        List<Expression> groupByExprs = selClause.getGroupByExprs();
        plan = processGroupByClause(plan, groupByExprs, processor);

        // Scan SELECT and HAVING clauses for aggregate functions, and rename SelectValues
        // to account for auto-generated column names.
        processAggregateFunctions(selClause, processor);
        processor.renameSelectValues(selClause.getSelectValues());

        Map<String, FunctionCall> aggregates = processor.getAggregateMap();
        if (!aggregates.isEmpty() || !groupByExprs.isEmpty()) {
            plan = new HashedGroupAggregateNode(plan, groupByExprs, aggregates);
        }

        if (selClause.getHavingExpr() != null) {
            plan = new SimpleFilterNode(plan, selClause.getHavingExpr());
        }

        return plan;
    }

    /**
     * Scan GROUP BY clause for complex expressions, map them to auto-generated column names
     * in the processor, and create a {@link edu.caltech.nanodb.plannodes.ProjectNode} for
     * these new columns.
     *
     * @param plan the child node of the resulting {@link edu.caltech.nanodb.plannodes.PlanNode}
     * @param groupByExprs list of <tt>GROUP BY</tt> expressions to process
     * @param processor processor in which the mappings are stored
     *
     * @return the resulting {@link edu.caltech.nanodb.plannodes.PlanNode}
     */
    public PlanNode processGroupByClause(PlanNode plan, List<Expression> groupByExprs,
                                         GroupAggregationProcessor processor) {
        if (groupByExprs != null && groupByExprs.size() > 0) {
            // Create mappings from auto-generated names to expressions
            List<SelectValue> groupByProjectionSpec = processor.processGroupByExprs(groupByExprs);
            if (groupByProjectionSpec.size() > 0) {
                // Add "*" wild card to include all other columns
                groupByProjectionSpec.add(new SelectValue(new ColumnName(null)));
                // Create GROUP BY columns using auto-generated names
                plan = new ProjectNode(plan, groupByProjectionSpec);
            }
        }

        return plan;
    }
}
