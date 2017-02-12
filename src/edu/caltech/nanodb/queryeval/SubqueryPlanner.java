package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.SubqueryOperator;
import edu.caltech.nanodb.expressions.InSubqueryOperator;
import edu.caltech.nanodb.expressions.ExistsOperator;
import edu.caltech.nanodb.plannodes.PlanNode;



public class SubqueryPlanner {

    SelectClause selClause;

    SelectClause subQuery;

    /** Subquery operator which we will set a plan to */
    SubqueryOperator subOp;

    public SubqueryPlanner(SelectClause selClause) {
        this.selClause = selClause;
    }

    public SelectClause parse() {
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            // EXISTS statement or IN statement in WHERE clause
            // TODO: not sure how to have planner diffrentiate between different cases
            subOp = (ExistsOperator) whereExpr;
            subQuery = subOp.getSubquery();
            return subQuery;
        }
        return null;
    }

    public void setPlan(PlanNode plan) {
        subOp.setSubqueryPlan(plan);
    }

}
