package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.SubqueryOperator;
import edu.caltech.nanodb.expressions.InSubqueryOperator;
import edu.caltech.nanodb.expressions.ExistsOperator;
import edu.caltech.nanodb.plannodes.PlanNode;


/** Class to plan for subqueries
 * Supports IN subquery in WHERE clause, EXISTS subquery in WHERE clause
 * and scalar subquery (1 col, 1 row) in SELECT clause
 */
public class SubqueryPlanner {

    /* Passed in parent select clause */
    SelectClause selClause;

    /* Subquery select clause */
    SelectClause subQuery;

    /* Subquery operator which we will set a plan to */
    SubqueryOperator subOp;

    public SubqueryPlanner(SelectClause selClause) {
        this.selClause = selClause;
    }

    /** Parse the passed in select clause to determine what subqueries there are
     * @return SelectClause of subquery
     *
     */
    public SelectClause parse() {
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            // EXISTS statement or IN statement in WHERE clause
            // TODO: not sure how to have planner diffrentiate between different cases
            // TODO: check for subquery in grouping or order by
            subOp = (ExistsOperator) whereExpr;
            subQuery = subOp.getSubquery();
            return subQuery;
        }
        return null;
    }

    /** Set the plan node for the subquery
     * @param PlanNode with plan for subquery
     *
    */
    public void setPlan(PlanNode plan) {
        subOp.setSubqueryPlan(plan);
    }

}
