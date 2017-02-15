package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;


import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.plannodes.HashedGroupAggregateNode;
import edu.caltech.nanodb.plannodes.ProjectNode;
import edu.caltech.nanodb.plannodes.SelectNode;
import edu.caltech.nanodb.plannodes.SimpleFilterNode;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.plannodes.NestedLoopJoinNode;
import edu.caltech.nanodb.plannodes.FileScanNode;
import edu.caltech.nanodb.plannodes.RenameNode;
import edu.caltech.nanodb.plannodes.SortNode;

import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;

import edu.caltech.nanodb.queryeval.SubqueryPlanner;

import edu.caltech.nanodb.relations.TableInfo;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);

    /**
     * Decorrelates two query types. First,
     * <pre>
     *   SELECT ...
     *   FROM t1 ...
     *   WHERE a IN (SELECT ... FROM t2 WHERE b = t1.c)
     * </pre>
     * is decorrelated into:
     * <pre>
     *   SELECT ...
     *   FROM t1 ... LEFT SEMIJOIN (SELECT ... FROM t2)
     *   ON b = t1.c AND a = ...
     * </pre>
     * Second,
     * <pre>
     *   SELECT ...
     *   FROM t1 ...
     *   WHERE EXISTS (SELECT ... FROM t2 WHERE a = t1.b)
     * </pre>
     * is decorrelated into:
     * <pre>
     *   SELECT ...
     *   FROM t1 ... LEFT SEMIJOIN (SELECT ... FROM t2)
     *   ON t2.a = t1.b
     * </pre>
     *
     * For both cases, we decorrelate only when the subquery is a simple
     * BASE_TABLE, since the decorrelated query requires an alias for the
     * nested SELECT. Rather than use a placeholder, we just use the table name.
     *
     * @param selClause the SELECT clause AST to decorrelate
     * @throws IOException
     */
    private void decorrelate(SelectClause selClause) throws IOException {
        Expression whereExpr = selClause.getWhereExpr();

        // SELECT ... FROM t1 ... WHERE a IN (SELECT ... FROM t2 WHERE b = t1.c)
        if (whereExpr instanceof InSubqueryOperator) {
            SelectClause subquery = ((InSubqueryOperator) whereExpr).getSubquery();

            if (subquery.getFromClause().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
                String tableName = subquery.getFromClause().getTableName();
                Expression predicate;

                // Tack on subquery WHERE expression
                predicate = subquery.getWhereExpr();
                subquery.setWhereExpr(null);

                // Tack on equality comparison for IN
                Expression lhs = ((InSubqueryOperator) whereExpr).expr;
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
                FromClause subqueryFromClause = new FromClause(subquery, tableName);

                // SEMIJOIN with ON condition
                FromClause newFromClause = new FromClause(fromClause, subqueryFromClause, JoinType.SEMIJOIN);
                newFromClause.setConditionType(FromClause.JoinConditionType.JOIN_ON_EXPR);
                newFromClause.setOnExpression(predicate);

                // Replace FROM clause and WHERE clause
                selClause.setFromClause(newFromClause);
                selClause.setWhereExpr(null);

                // Recompute schema
                selClause.computeSchema(storageManager.getTableManager(), null);
            }
        }

        // SELECT ... FROM t1 ... WHERE EXISTS (SELECT ... FROM t2 WHERE a = t1.b)
        if (whereExpr instanceof ExistsOperator) {
            SelectClause subquery = ((ExistsOperator) whereExpr).getSubquery();

            if (subquery.getFromClause().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
                String tableName = subquery.getFromClause().getTableName();
                Expression subWhereExpr = subquery.getWhereExpr();
                subquery.setWhereExpr(null);

                // Left and right FROM clauses
                FromClause fromClause = selClause.getFromClause();
                FromClause subqueryFromClause = new FromClause(subquery, tableName);

                // SEMIJOIN with ON condition
                FromClause newFromClause = new FromClause(fromClause, subqueryFromClause, JoinType.SEMIJOIN);
                if (subWhereExpr != null) {
                    newFromClause.setConditionType(FromClause.JoinConditionType.JOIN_ON_EXPR);
                    newFromClause.setOnExpression(subWhereExpr);
                }

                // Replace FROM clause and WHERE clause
                selClause.setFromClause(newFromClause);
                selClause.setWhereExpr(null);

                // Recompute schema
                selClause.computeSchema(storageManager.getTableManager(), null);
            }
        }


    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // Decorrelate the query AST before doing anything else
        decorrelate(selClause);

        FromClause fromClause = selClause.getFromClause();
        Expression whereExpr = selClause.getWhereExpr();
        PlanNode plan;
        ArrayList<SelectClause> subqueries;

        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(selClause, this, enclosingSelects);

        // Process FROM clause
        if (fromClause == null) {
            logger.debug("No from clause");
            plan = new ProjectNode(selClause.getSelectValues());
        } else if (fromClause.isBaseTable()) {
            logger.debug("Using SimpleSelect: " + fromClause.toString());
            plan = makeSimpleSelect(fromClause.getTableName(),
                    whereExpr, enclosingSelects);
            whereExpr = null;   // Set to null to avoid double filtering
            // For when AS is used to rename table from a FROM clause
            if (fromClause.isRenamed()) {
                plan = new RenameNode(plan, fromClause.getResultName());
            }
        } else {
            logger.debug("From clause: "  + fromClause.toString());
            plan = processFromClause(fromClause, true);
        }

        // Process WHERE clause
        if (whereExpr != null) {
            logger.debug("Where clause: " + whereExpr.toString());
            // Check that WHERE clause does not contain aggregate functions
            if (containsAggregateFunction(whereExpr)) {
                throw new IllegalArgumentException("Where clauses cannot contain aggregation " +
                        "functions");
            }
            plan = new SimpleFilterNode(plan, whereExpr);
        }

        if (subqueryPlanner.scanWhereExpr()) {
            plan.setEnvironment(subqueryPlanner.environment);
            subqueryPlanner.reset();
        }

        // Grouping and Aggregation
        // This may modify selClause to account for aggregate functions and
        // complex expressions in GROUP BY clauses.
        plan = processGroupAggregation(plan, selClause);
        if (subqueryPlanner.scanHavingExpr()) {
            plan.setEnvironment(subqueryPlanner.environment);
            subqueryPlanner.reset();
        }

        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selClause.getSelectValues());
            if (subqueryPlanner.scanSelectValues()) {
                plan.setEnvironment(subqueryPlanner.environment);
                subqueryPlanner.reset();
            }
        }

        // Process ORDER BY clause
        List<OrderByExpression> orderByClause = selClause.getOrderByExprs();
        if (!orderByClause.isEmpty()) {
            logger.debug("Order by clause: " + orderByClause);
            plan = new SortNode(plan, orderByClause);
        }

        plan.prepare();
        return plan;
    }

    /**
     * Returns a PlanNode based on what is contained within the <tt>FROM</tt> clause.
     *
     *
     * @param fromClause The <tt>FROM</tt> clause that we are processing.
     *
     * @return A new PlanNode with the contents of the processed <tt>FROM</tt> clause.
     *
     * @throws IllegalArgumentException if an ON clause contains aggregate functions
     * @throws UnsupportedOperationException if the clause type is unsupported
     */
    public PlanNode processFromClause(FromClause fromClause,
              boolean isTopFrom) throws IOException {
        PlanNode plan;

        switch (fromClause.getClauseType()) {
            // No enclosing selects or predicate
            case BASE_TABLE: {
                String baseTableName = fromClause.getTableName();
                logger.debug("Base Table: " + baseTableName);
                plan = makeSimpleSelect(baseTableName, null, null);
                break;
            }

            // Such as: SELECT * FROM (SELECT ...)
            case SELECT_SUBQUERY: {
                SelectClause innerSelect = fromClause.getSelectClause();
                logger.debug("Select Subquery: " + innerSelect.toString());

                // No enclosing selects
                // TODO unsure if should be using enclosingSelects = null
                plan = makePlan(innerSelect, null);
                break;
            }

            // Such as: SELECT * FROM table1 AS t1 INNER JOIN table2 AS t2 ...
            case JOIN_EXPR: {
                logger.debug("Join query: " + fromClause.getJoinType());

                FromClause.JoinConditionType condType = fromClause.getConditionType();

                // Check that ON clause does not contain aggregate functions
                if (condType == FromClause.JoinConditionType.JOIN_ON_EXPR) {
                    if (containsAggregateFunction(fromClause.getOnExpression())) {
                        throw new IllegalArgumentException("ON clauses cannot contain aggregate " +
                                "functions");
                    }
                }

                // Recursively process left and right children with this method.
                plan = new NestedLoopJoinNode(
                                processFromClause(fromClause.getLeftChild(), false),
                                processFromClause(fromClause.getRightChild(), false),
                                fromClause.getJoinType(),
                                fromClause.getComputedJoinExpr());

                // For NATURAL joins and joins with USING, project onto
                // precalculated schema that removes duplicate columns.
                if (condType == FromClause.JoinConditionType.NATURAL_JOIN ||
                        condType == FromClause.JoinConditionType.JOIN_USING) {
                    List<SelectValue> computedSelectValues = fromClause.getComputedSelectValues();
                    if (computedSelectValues != null) {
                        // Remove aliases for all nodes except the topmost
                        // ProjectNode. Removing aliases keeps the table names
                        // in the column names, thus eliminating ambiguity.
                        // The topmost ProjectNode will keep the aliases which
                        // is fine because there are no more ambiguous joins.
                        if (!isTopFrom) {
                            for (int i = 0; i < computedSelectValues.size(); i++) {
                                logger.debug("selectValues " + i + ": " + computedSelectValues.get(i));
                                SelectValue value = computedSelectValues.get(i);
                                Expression exp = value.getExpression();
                                SelectValue newValue = new SelectValue(exp, null);
                                computedSelectValues.set(i, newValue);
                            }
                        }

                        plan = new ProjectNode(plan, computedSelectValues);
                    }
                }

                break;
            }

            // Unimplemented or unsupported at the moment
            case TABLE_FUNCTION:
            default:
                throw new UnsupportedOperationException("Invalid FROM clause");
        }

        // For when AS is used to rename table from a FROM clause
        if (fromClause.isRenamed()) {
            plan = new RenameNode(plan, fromClause.getResultName());
        }

        return plan;
    }

    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        return selectNode;
    }
}
