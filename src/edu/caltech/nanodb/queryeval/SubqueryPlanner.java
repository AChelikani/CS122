package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.SubqueryOperator;
import edu.caltech.nanodb.expressions.ScalarSubquery;
import edu.caltech.nanodb.expressions.InSubqueryOperator;
import edu.caltech.nanodb.expressions.ExistsOperator;
import edu.caltech.nanodb.plannodes.PlanNode;
import java.util.List;
import java.util.ArrayList;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.expressions.OrderByExpression;



/** Class to plan for subqueries
 * Supports IN subquery in WHERE clause, EXISTS subquery in WHERE clause
 * and scalar subquery (1 col, 1 row) in SELECT clause
 * Throws exception when subquery in order by or group by clause is detected
 */
public class SubqueryPlanner {

    /* Passed in parent select clause */
    SelectClause selClause;

    /* List of subqueries */
    ArrayList<SelectClause> subqueries = new ArrayList<SelectClause>();

    /* List of subquery operators */
    ArrayList<SubqueryOperator> subOps = new ArrayList<SubqueryOperator>();


    public SubqueryPlanner(SelectClause selClause) {
        this.selClause = selClause;
    }

    /** Parse the passed in select clause to determine what subqueries there are
     * @return SelectClause of subquery
     *
     */
    public ArrayList<SelectClause> parse() {
        Expression whereExpr = selClause.getWhereExpr();
        Expression havingExpr = selClause.getHavingExpr();
        List<Expression> groupByExprs = selClause.getGroupByExprs();
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        List<SelectValue> selectValues = selClause.getSelectValues();
        // Check if subqueries in group by or order by expressions
        for (Expression e : groupByExprs) {
            if (e instanceof SubqueryOperator) {
                throw new UnsupportedOperationException("Not a valid place to put subquery");
            }
        }
        for (OrderByExpression e : orderByExprs) {
            if (e.getExpression() instanceof SubqueryOperator) {
                throw new UnsupportedOperationException("Not a valid place to put subqyery");
            }
        }
        // Check if there is a scalar subquery
        if (selectValues.size() == 1 && selectValues.get(0).isScalarSubquery()) {
            subOps.add((ScalarSubquery) selectValues.get(0).getExpression());
        }
        // Checks for subqueries in where expression
        if (whereExpr != null) {
            // EXISTS statement or IN statement in WHERE clause
            if (whereExpr instanceof ExistsOperator) {
                subOps.add((ExistsOperator) whereExpr);
            } else if (whereExpr instanceof InSubqueryOperator) {
                subOps.add((InSubqueryOperator) whereExpr);
            }
        }
        // Checks for subqueries in having expression
        if (havingExpr != null) {
            if (havingExpr instanceof ExistsOperator) {
                subOps.add((ExistsOperator) havingExpr);
            } else if (havingExpr instanceof InSubqueryOperator) {
                subOps.add((InSubqueryOperator) havingExpr);
            }
        }
        // Computes all subqueries from operators
        for (SubqueryOperator so : subOps) {
            subqueries.add(so.getSubquery());
        }
        return subqueries;
    }

    /** Set the plan node for the subquery
     * @param PlanNode with plan for subquery
     *
    */
    public void setPlan(PlanNode plan, int i) {
        subOps.get(i).setSubqueryPlan(plan);
    }

}
