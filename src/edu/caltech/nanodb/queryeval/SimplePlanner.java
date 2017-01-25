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

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }

        FromClause fromClause = selClause.getFromClause();
        PlanNode plan;

        // Process FROM clause
        if (fromClause == null) {
            logger.debug("No from clause");
            plan = new ProjectNode(selClause.getSelectValues());
        } else {
            logger.debug("From clause: " + fromClause.toString());
            plan = processFromClause(fromClause, true);
        }

        // Process WHERE clause
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            logger.debug("Where clause: " + whereExpr.toString());
            // Check that WHERE clause does not contain aggregate functions
            if (containsAggregateFunction(whereExpr)) {
                throw new IllegalArgumentException("Where clauses cannot contain aggregation " +
                        "functions");
            }
            plan = new SimpleFilterNode(plan, whereExpr);
        }

        // Grouping and Aggregation
        // This may modify selClause to account for aggregate functions and
        // complex expressions in GROUP BY clauses.
        plan = processGroupAggregation(plan, selClause);

        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selClause.getSelectValues());
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
                        // Remove placeholders for the topmost ProjectNode
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
