package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.GroupAggregationProcessor;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.FromClause.ClauseType;
import edu.caltech.nanodb.queryast.SelectClause;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.GroupAggregationProcessor;

import edu.caltech.nanodb.plannodes.SelectNode;

import edu.caltech.nanodb.plannodes.SimpleFilterNode;
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
        // GROUPING and AGGREGATION

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
            logger.debug("From clause");
            plan = processFromClause(fromClause);
            // For when AS is used to rename table from a FROM clause
            if (fromClause.isRenamed()) {
	    		plan = new RenameNode(plan, fromClause.getResultName());
        	}
        }

        // Where clause
        if (selClause.getWhereExpr() != null) {
            logger.debug("Where clause");
            plan = new SimpleFilterNode(plan, selClause.getWhereExpr());
        }

        /*
            Todo for Aggregation:
            #1. Implement custom ExpressionProcessor
                #1. Map from auto-generated -> aggregate function

            First (Group by x where x is 1 column):
                1. Scan SELECT clauses to identify all aggregate functions
                2. Replace with auto-generated column references
                3. Group and aggregate all functions identified above
                4. Project using correct select values and correct names

                Add-on: Also scan HAVING clauses

            Next (Group by x where x can be an expression):
                1. When scanning, also scan grouping expressions for expressions like (a-b)

            IllegalArgumentException on:
                1. Make sure WHERE and ON clauses don't contain aggregates using ExpressionProcessor
                2. No Aggregate inside aggregate

            References:
                1. Expression class traverse()
                2. Expression Processor
                3. FunctionCall.getFunction()
                4. ColumnValue class (replacing function call with column-reference)
                5. HashedGroupAggregateNode

         */

        // Group and Aggregation
        if (!selClause.getGroupByExprs().isEmpty()) {
            GroupAggregationProcessor processor = new GroupAggregationProcessor();

            List<SelectValue> selectValues = selClause.getSelectValues();
            for (SelectValue sv : selectValues) {
                if (!sv.isExpression()) {
                    continue;
                }
                Expression e = sv.getExpression().traverse(processor);
                sv.setExpression(e);
            }

            // Todo: Scan HAVING

            Map<String, FunctionCall> aggregates = processor.getAggregateMap();

            plan = new HashedGroupAggregateNode(plan, selClause.getGroupByExprs(), aggregates);

        }

        // For selects that are not select *
        else if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selClause.getSelectValues());
        }

        // If order by clause
        if (!selClause.getOrderByExprs().isEmpty()) {
            logger.debug("Order by clause");
            plan = new SortNode(plan, selClause.getOrderByExprs());
        }

        plan.prepare();
        return plan;
    }

    /**
     * Returns a PlanNode based on what is contained within the FROM clause.
     *
     *
     * @param FromClause The FROM clause that we are processing.
     *
     * @return A new PlanNode with the contnets of the processed FROM clause.
     *
     * @throws IOException if an error occurs because of invalid FROM clause.
     */
    public PlanNode processFromClause(FromClause fromClause) throws IOException {
        PlanNode plan;
        if (fromClause.getClauseType() == ClauseType.BASE_TABLE) {
            logger.debug("Base Table");
            // No enclosing selects or predicate
            plan = makeSimpleSelect(fromClause.getTableName(), null, null);
        } else if (fromClause.getClauseType() == ClauseType.SELECT_SUBQUERY) {
            logger.debug("Select Subquery");
            // No enclosing selects
            plan = makePlan(fromClause.getSelectClause(), null);
        } else if (fromClause.getClauseType() == ClauseType.JOIN_EXPR) {
            logger.debug("Join query");
            plan = new NestedLoopJoinNode(
                processFromClause(fromClause.getLeftChild()),
                processFromClause(fromClause.getRightChild()),
                fromClause.getJoinType(),
                fromClause.getComputedJoinExpr());
        } else {
            throw new UnsupportedOperationException("Invalid FROM clause");
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
