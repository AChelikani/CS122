package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plannodes.HashedGroupAggregateNode;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.plannodes.ProjectNode;
import edu.caltech.nanodb.plannodes.SimpleFilterNode;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;
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

    /**
     * Decorrelates the
     * <pre>
     *   SELECT ...
     *   FROM t1 ...
     *   WHERE a [NOT] IN (SELECT ... FROM t2 WHERE b = t1.c)
     * </pre>
     * case.
     *
     * @param selClause the outer SELECT clause to decorrelate
     * @param inExpr the IN expression to decorrelate
     * @param notIn boolean flag for if this is NOT IN
     * @throws IOException
     */
    private void decorrelateInSubquery(SelectClause selClause,
                                       InSubqueryOperator inExpr,
                                       boolean notIn) throws IOException {
        SelectClause subquery = inExpr.getSubquery();

        if (subquery.getFromClause().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
            String resultName = subquery.getFromClause().getResultName();
            JoinType joinType = notIn ? JoinType.ANTIJOIN : JoinType.SEMIJOIN;

            // Predicate starts off as the WHERE expression in the subquery
            Expression predicate = subquery.getWhereExpr();
            subquery.setWhereExpr(null);

            // Tack on equality comparison for IN clause (i.e. a = t2.b)
            Expression lhs = inExpr.expr;
            Expression rhs = new ColumnValue(subquery.getSchema().getColumnInfo(0).getColumnName());
            Expression inCompare = new CompareOperator(CompareOperator.Type.EQUALS, lhs, rhs);
            if (predicate == null) {
                predicate = inCompare;
            } else {
                BooleanOperator combined = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
                combined.addTerm(predicate);
                combined.addTerm(inCompare);
                predicate = combined;
            }

            // Left and right FROM clauses
            FromClause fromClause = selClause.getFromClause();
            FromClause subqueryFromClause = new FromClause(subquery, resultName);

            // SEMIJOIN or ANTIJOIN with ON condition
            FromClause newFromClause = new FromClause(fromClause, subqueryFromClause, joinType);
            newFromClause.setConditionType(FromClause.JoinConditionType.JOIN_ON_EXPR);
            newFromClause.setOnExpression(predicate);

            // Replace FROM clause and WHERE clause
            selClause.setFromClause(newFromClause);
            selClause.setWhereExpr(null);

            // Recompute schema
            selClause.computeSchema(storageManager.getTableManager(), null);
        }
    }

    /**
     * Decorrelates the
     * <pre>
     *   SELECT ...
     *   FROM t1 ...
     *   WHERE [NOT] EXISTS (SELECT ... FROM t2 WHERE a = t1.b)
     * </pre>
     * case.
     *
     * @param selClause the outer SELECT clause to decorrelate
     * @param existExpr the EXISTS expression to decorrelate
     * @param notExists boolean flag for if this is NOT EXISTS
     * @throws IOException
     */
    private void decorrelateExists(SelectClause selClause,
                                   ExistsOperator existExpr,
                                   boolean notExists) throws IOException {
        SelectClause subquery = existExpr.getSubquery();

        if (subquery.getFromClause().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
            String resultName = subquery.getFromClause().getResultName();
            JoinType joinType = notExists ? JoinType.ANTIJOIN : JoinType.SEMIJOIN;

            // Predicate is simply the WHERE expression in the subquery
            Expression predicate = subquery.getWhereExpr();
            subquery.setWhereExpr(null);

            // Left and right FROM clauses
            FromClause fromClause = selClause.getFromClause();
            FromClause subqueryFromClause = new FromClause(subquery, resultName);

            // SEMIJOIN or ANTIJOIN with ON condition
            FromClause newFromClause = new FromClause(fromClause, subqueryFromClause, joinType);
            if (predicate != null) {
                newFromClause.setConditionType(FromClause.JoinConditionType.JOIN_ON_EXPR);
                newFromClause.setOnExpression(predicate);
            }

            // Replace FROM clause and WHERE clause
            selClause.setFromClause(newFromClause);
            selClause.setWhereExpr(null);

            // Recompute schema
            selClause.computeSchema(storageManager.getTableManager(), null);
        }
    }


    /**
     * Decorrelates two query types. First,
     * <pre>
     *   SELECT ...
     *   FROM t1 ...
     *   WHERE a [NOT] IN (SELECT ... FROM t2 WHERE b = t1.c)
     * </pre>
     * is decorrelated into:
     * <pre>
     *   SELECT ...
     *   FROM t1 ... LEFT SEMIJOIN[ANTIJOIN] (SELECT ... FROM t2)
     *   ON b = t1.c AND a = ...
     * </pre>
     * Second,
     * <pre>
     *   SELECT ...
     *   FROM t1 ...
     *   WHERE [NOT] EXISTS (SELECT ... FROM t2 WHERE a = t1.b)
     * </pre>
     * is decorrelated into:
     * <pre>
     *   SELECT ...
     *   FROM t1 ... LEFT SEMIJOIN[ANTIJOIN] (SELECT ... FROM t2)
     *   ON t2.a = t1.b
     * </pre>
     *
     * For both cases, we decorrelate only when the subquery is a simple
     * BASE_TABLE, since the decorrelated query requires an alias for the
     * nested SELECT. Rather than use a placeholder, we just use the table name.
     *
     * This method modifies the selClause in place.
     *
     * @param selClause the SELECT clause AST to decorrelate
     * @throws IOException
     */
    protected void decorrelate(SelectClause selClause) throws IOException {
        Expression whereExpr = selClause.getWhereExpr();

        if (whereExpr instanceof InSubqueryOperator) {
            // SELECT ... FROM t1 ... WHERE a IN (SELECT ... FROM t2 WHERE b = t1.c)
            decorrelateInSubquery(selClause, (InSubqueryOperator) whereExpr, false);
        }

        else if (whereExpr instanceof ExistsOperator) {
            // SELECT ... FROM t1 ... WHERE EXISTS (SELECT ... FROM t2 WHERE a = t1.b)
            decorrelateExists(selClause, (ExistsOperator) whereExpr, false);
        }

        else if (whereExpr instanceof BooleanOperator &&
                ((BooleanOperator) whereExpr).getType() == BooleanOperator.Type.NOT_EXPR) {
            // Extract inner expression and check if we can decorrelate it
            Expression expr = ((BooleanOperator) whereExpr).getTerm(0);

            if (expr instanceof  InSubqueryOperator) {
                // SELECT ... FROM t1 ... WHERE a NOT IN (SELECT ... FROM t2 WHERE b = t1.c)
                decorrelateInSubquery(selClause, (InSubqueryOperator) expr, true);
            }

            else if (expr instanceof  ExistsOperator) {
                // SELECT ... FROM t1 ... WHERE NOT EXISTS (SELECT ... FROM t2 WHERE a = t1.b)
                decorrelateExists(selClause, (ExistsOperator) expr, true);
            }
        }
    }
}
