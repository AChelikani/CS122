package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.plannodes.PlanNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import edu.caltech.nanodb.queryast.SelectValue;


/** Class to plan for subqueries
 * Supports IN subquery in WHERE clause, EXISTS subquery in WHERE clause
 * and scalar subquery (1 col, 1 row) in SELECT clause
 * Throws exception when subquery in ORDER BY or GROUP BY clause is detected
 */
public class SubqueryPlanner {

    private static class SubqueryFinder implements ExpressionProcessor {

        ArrayList<SubqueryOperator> subqueryOperators;

        public SubqueryFinder() {
            subqueryOperators = new ArrayList<>();
        }

        public void enter(Expression e) {
            if (e instanceof ScalarSubquery ||
                    e instanceof ExistsOperator ||
                    e instanceof InSubqueryOperator) {
                subqueryOperators.add((SubqueryOperator) e);
            }
        }

        public Expression leave(Expression e) {
            // This function never changes the node that is traversed.
            return e;
        }
    }

    public Environment environment;

    private SelectClause selClause;
    private AbstractPlannerImpl parentPlanner;
    private List<SelectClause> enclosingSelects;

    public SubqueryPlanner(SelectClause selectClause,
                           AbstractPlannerImpl parent,
                           List<SelectClause> parentEnclosingSelects) {
        selClause = selectClause;
        parentPlanner = parent;
        environment = new Environment();

        if (parentEnclosingSelects == null) {
            enclosingSelects = new ArrayList<SelectClause>();
        }
        else {
            enclosingSelects = new ArrayList<SelectClause>(parentEnclosingSelects);
        }
        enclosingSelects.add(selClause);

        validateGroupByExpr();
        validateOrderByExpr();
    }

    public boolean scanSelectValues() throws IOException {
        List<SelectValue> selectValues = selClause.getSelectValues();
        boolean subqueryFound = false;

        for (SelectValue sv : selectValues) {
            Expression expr = sv.getExpression();
            if (expr instanceof ScalarSubquery) {
                SubqueryOperator subqueryOp = (SubqueryOperator) expr;
                PlanNode plan = parentPlanner.makePlan(subqueryOp.getSubquery(), enclosingSelects);
                plan.addParentEnvironmentToPlanTree(environment);
                subqueryOp.setSubqueryPlan(plan);
                subqueryFound = true;
            }
        }
        return subqueryFound;
    }

    public boolean scanWhereExpr() throws IOException {
        return scanExpr(selClause.getWhereExpr());
    }

    public boolean scanHavingExpr() throws IOException {
        return scanExpr(selClause.getHavingExpr());
    }

    private boolean scanExpr(Expression e) throws IOException {
        if (e == null)
            return false;
        SubqueryFinder subqueryFinder = new SubqueryFinder();
        e.traverse(subqueryFinder);

        if (subqueryFinder.subqueryOperators.isEmpty()) {
            return false;
        }

        for (SubqueryOperator subqueryOp : subqueryFinder.subqueryOperators) {
            PlanNode plan = parentPlanner.makePlan(subqueryOp.getSubquery(), enclosingSelects);
            plan.addParentEnvironmentToPlanTree(environment);
            subqueryOp.setSubqueryPlan(plan);
        }
        return true;
    }

    private void validateGroupByExpr() {
        List<Expression> groupByExprs = selClause.getGroupByExprs();
        for (Expression e : groupByExprs) {
            if (e instanceof SubqueryOperator) {
                throw new UnsupportedOperationException("Not a valid place to put subquery");
            }
        }
    }

    private void validateOrderByExpr() {
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        for (OrderByExpression e : orderByExprs) {
            if (e.getExpression() instanceof SubqueryOperator) {
                throw new UnsupportedOperationException("Not a valid place to put subquery");
            }
        }
    }

    public void reset() {
        environment = new Environment();
    }
}
