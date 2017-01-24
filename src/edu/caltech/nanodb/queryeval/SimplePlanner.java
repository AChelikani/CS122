package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.Map;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.GroupAggregationProcessor;
import edu.caltech.nanodb.expressions.OrderByExpression;

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
import edu.caltech.nanodb.queryast.FromClause.ClauseType;
import edu.caltech.nanodb.queryast.SelectClause;

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

        // TODO:
        // Confirm JOINS work once NestedLoopJoinNode is done

        // For HW1, we have a very simple implementation that defers to
        // makeSimpleSelect() to handle simple SELECT queries with one table,
        // and an optional WHERE clause.

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }

        FromClause fromClause = selClause.getFromClause();
        PlanNode plan;

        // Depending on if there is a from clause
        if (fromClause == null) {
            logger.debug("No from clause");
            plan = new ProjectNode(selClause.getSelectValues());
        } else {
            logger.debug("From clause: " + fromClause.toString());
            plan = processFromClause(fromClause);
        }

        // Where clause
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            logger.debug("Where clause: " + whereExpr.toString());
            if (containsAggregateFunction(whereExpr)) {
                throw new IllegalArgumentException("Where clauses cannot contain aggregation " +
                        "functions");
            }
            plan = new SimpleFilterNode(plan, whereExpr);
        }

        /*
            Todo for Grouping and Aggregation:

            1. Scan GROUP BY expressions

         */

        // Grouping and Aggregation

        GroupAggregationProcessor processor = new GroupAggregationProcessor();

        List<Expression> groupByExprs = selClause.getGroupByExprs();
        if (groupByExprs.size() > 0) {
            // Scan GROUP BY clause
            List<SelectValue> groupByProjectionSpec
                    = processor.processGroupByExprs(groupByExprs);
            if (groupByProjectionSpec.size() > 0) {
                groupByProjectionSpec.add(new SelectValue(new ColumnName(null)));
                plan = new ProjectNode(plan, groupByProjectionSpec);
            }
        }

        // Scan SELECT clause for aggregate functions
        List<SelectValue> selectValues = selClause.getSelectValues();
        for (SelectValue sv : selectValues) {
            if (!sv.isExpression()) {
                continue;
            }
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        // Scan HAVING clause
        Expression havingExpr = selClause.getHavingExpr();
        Expression newHavingExpr = null;
        if (havingExpr != null) {
            newHavingExpr = havingExpr.traverse(processor);
        }

        processor.renameSelectValues(selectValues);
        Map<String, FunctionCall> aggregates = processor.getAggregateMap();

        if (!aggregates.isEmpty() || !selClause.getGroupByExprs().isEmpty()) {
            plan = new HashedGroupAggregateNode(plan, selClause.getGroupByExprs(), aggregates);
        }

        if (newHavingExpr != null) {
            plan = new SimpleFilterNode(plan, newHavingExpr);
        }

        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selectValues);
        }

        // If order by clause
        List<OrderByExpression> orderByClause = selClause.getOrderByExprs();
        if (!orderByClause.isEmpty()) {
            logger.debug("Order by clause: " + orderByClause);
            plan = new SortNode(plan, orderByClause);
        }

        plan.prepare();
        return plan;
    }

    /**
     * Returns a PlanNode based on what is contained within the FROM clause.
     *
     *
     * @param fromClause The FROM clause that we are processing.
     *
     * @return A new PlanNode with the contnets of the processed FROM clause.
     *
     * @throws IOException if an error occurs because of invalid FROM clause.
     */
    public PlanNode processFromClause(FromClause fromClause) throws IOException {

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
                                processFromClause(fromClause.getLeftChild()),
                                processFromClause(fromClause.getRightChild()),
                                fromClause.getJoinType(),
                                fromClause.getComputedJoinExpr());

                // For NATURAL joins and joins with USING, project onto
                // precalculated schema that removes duplicate columns.
                if (condType == FromClause.JoinConditionType.NATURAL_JOIN ||
                        condType == FromClause.JoinConditionType.JOIN_USING) {
                    plan = new ProjectNode(plan, fromClause.getComputedSelectValues());
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
     * DOCUMENTATION NEEDED
     */
    public boolean containsAggregateFunction(Expression e) {
        GroupAggregationProcessor processor = new GroupAggregationProcessor();
        e.traverse(processor);
        return !processor.getAggregateMap().isEmpty();
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

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}