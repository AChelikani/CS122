package edu.caltech.nanodb.queryeval;


import java.io.IOException;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Predicate;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.PredicateUtils;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
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
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        //
        // This is a very rough sketch of how this function will work,
        // focusing mainly on join planning:
        //
        // 1)  Pull out the top-level conjuncts from the WHERE and HAVING
        //     clauses on the query, since we will handle them in special ways
        //     if we have outer joins.
        //
        // 2)  Create an optimal join plan from the top-level from-clause and
        //     the top-level conjuncts.
        //
        // 3)  If there are any unused conjuncts, determine how to handle them.
        //
        // 4)  Create a project plan-node if necessary.
        //
        // 5)  Handle other clauses such as ORDER BY, LIMIT/OFFSET, etc.
        //
        // Supporting other query features, such as grouping/aggregation,
        // various kinds of subqueries, queries without a FROM clause, etc.,
        // can all be incorporated into this sketch relatively easily.

        // Decorrelate the query AST before doing anything else
        decorrelate(selClause);

        // 0) The Declaration of Variables
        FromClause fromClause = selClause.getFromClause();
        Expression whereExpr = selClause.getWhereExpr();
        Expression havingExpr = selClause.getHavingExpr();
        PlanNode plan;

        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(selClause, this, enclosingSelects);

        // 1) Pull conjuncts from WHERE and HAVING
        if (whereExpr != null) {
            logger.debug("Where clause: " + whereExpr.toString());

            // Check that WHERE clause does not contain aggregate functions
            if (containsAggregateFunction(whereExpr)) {
                throw new IllegalArgumentException("Where clauses cannot contain aggregation " +
                        "functions");
            }

            // Pull all conjuncts
            PredicateUtils.collectConjuncts(whereExpr, conjuncts);
        }

        if (havingExpr != null) {
            logger.debug("Having clause: " + havingExpr.toString());
            // Pull all conjuncts that do not contain aggregate functions
            ArrayList<Expression> havingConjuncts = new ArrayList<>();
            PredicateUtils.collectConjuncts(whereExpr, havingConjuncts);
            for (Expression expr : havingConjuncts) {
                if(!containsAggregateFunction(expr)) {
                    conjuncts.add(expr);
                }
            }
        }

        // 2) Create an optimal join plan from top-level from-clause and conjuncts
        if (fromClause == null) {
            logger.debug("No from clause");
            plan = new ProjectNode(selClause.getSelectValues());
        } else {
            logger.debug("From clause: " + fromClause.toString());

            collectDetails(fromClause, conjuncts, leafFromClauses);
            ArrayList<JoinComponent> leaves = generateLeafJoinComponents(leafFromClauses, conjuncts);
            JoinComponent topJC = generateOptimalJoin(leaves, conjuncts);

            conjuncts.removeAll(topJC.conjunctsUsed);
            plan = topJC.joinPlan;

            // For NATURAL and joins with USING clause, project back onto appropriate schema
            if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR) {
                FromClause.JoinConditionType condType = fromClause.getConditionType();
                if (condType == FromClause.JoinConditionType.NATURAL_JOIN ||
                        condType == FromClause.JoinConditionType.JOIN_USING) {
                    List<SelectValue> computedSelectValues = fromClause.getComputedSelectValues();
                    if (computedSelectValues != null) {
                        plan = new ProjectNode(plan, computedSelectValues);
                    }
                }
            }
        }

        // 3) Handle unused conjuncts
        if (!conjuncts.isEmpty()) {
            Expression predicate = PredicateUtils.makePredicate(conjuncts);
            plan = new SimpleFilterNode(plan, predicate);
        }

        if (subqueryPlanner.scanWhereExpr()) {
            plan.setEnvironment(subqueryPlanner.environment);
            subqueryPlanner.reset();
        }


        // 3.5) Grouping and Aggregation
        // This may modify selClause to account for aggregate functions and
        // complex expressions in GROUP BY clauses.
        plan = processGroupAggregation(plan, selClause);
        if (subqueryPlanner.scanHavingExpr()) {
            plan.setEnvironment(subqueryPlanner.environment);
            subqueryPlanner.reset();
        }


        // 4) Project plan node
        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selClause.getSelectValues());
            if (subqueryPlanner.scanSelectValues()) {
                plan.setEnvironment(subqueryPlanner.environment);
                subqueryPlanner.reset();
            }
        }


        // 5) Handle other clauses (ORDER BY, LIMIT/OFFSET, etc.)
        List<OrderByExpression> orderByClause = selClause.getOrderByExprs();
        if (!orderByClause.isEmpty()) {
            logger.debug("Order by clause: " + orderByClause);
            plan = new SortNode(plan, orderByClause);
        }

        plan.prepare();
        return plan;
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:\n" +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * Checks from clause for a base table, select subquery, or join expressions
     * and adds the from clauses to the list of leaves and the conjuncts to the
     * list of conjuncts.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) throws IOException {
        // Needs to handle base table, subqueries, outer joins as leaves
        // Collect leafs and conjuncts
        // Can model off of processFromClause in SimplePlanner
        switch (fromClause.getClauseType()) {
            case BASE_TABLE:
            case SELECT_SUBQUERY:
                leafFromClauses.add(fromClause);
                break;
            case JOIN_EXPR:
                // Keep outer joins, semijoins, antijoins as leaves
                if (fromClause.isOuterJoin() ||
                        fromClause.getJoinType() == JoinType.SEMIJOIN ||
                        fromClause.getJoinType() == JoinType.ANTIJOIN) {
                    leafFromClauses.add(fromClause);
                }
                // Recurse on left and right child
                else {
                    // Get conjuncts from the join expression
                    PredicateUtils.collectConjuncts(fromClause.getComputedJoinExpr(), conjuncts);
                    // Recurse on children of join
                    collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
                    collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported FROM clause");
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts
     *        applied in this plan from the <tt>conjuncts</tt> collection
     *        should be added to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        //        If you apply any conjuncts then make sure to add them to the
        //        leafConjuncts collection.
        //
        //        Don't forget that all from-clauses can specify an alias.
        //
        //        Concentrate on properly handling cases other than outer
        //        joins first, then focus on outer joins once you have the
        //        typical cases supported.

        PlanNode plan = null;

        switch (fromClause.getClauseType()) {
            case BASE_TABLE:
                // Create a FileScanNode
                String tableName = fromClause.getTableName();
                TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);
                plan = new FileScanNode(tableInfo, null);
                break;
            case SELECT_SUBQUERY:
                // Recursively get the plan node for the subquery
                plan = makePlan(fromClause.getSelectClause(), null);
                break;
            case JOIN_EXPR:
                if (!fromClause.isOuterJoin() &&
                        !(fromClause.getJoinType() == JoinType.SEMIJOIN) &&
                        !(fromClause.getJoinType() == JoinType.ANTIJOIN)) {
                    throw new IllegalArgumentException("Specified FROM clause is a join " +
                            "expression that isn't an outer/semi/anti join");
                }
                // Compute child join components for outer joins
                JoinComponent leftComponent;
                JoinComponent rightComponent;

                // Pass down conjuncts if the opposite child is not the "outer" side
                leftComponent = makeJoinPlan(fromClause.getLeftChild(),
                        fromClause.hasOuterJoinOnRight() ? null : conjuncts);
                rightComponent = makeJoinPlan(fromClause.getRightChild(),
                        fromClause.hasOuterJoinOnLeft() ? null : conjuncts);
                leafConjuncts.addAll(leftComponent.conjunctsUsed);
                leafConjuncts.addAll(rightComponent.conjunctsUsed);

                // Outer join the two children
                plan = new NestedLoopJoinNode(leftComponent.joinPlan, rightComponent.joinPlan,
                        fromClause.getJoinType(), fromClause.getComputedJoinExpr());
                break;
            default:
                throw new IllegalArgumentException("Unrecognized FROM clause type");
        }

        // Prepare plan to set its schema
        plan.prepare();

        // Get conjuncts that have not been used yet. Conjuncts may
        // only have been used if this is an outer join.
        HashSet<Expression> unusedConjuncts = new HashSet<>(conjuncts);
        unusedConjuncts.removeAll(leafConjuncts);

        // Compute conjuncts that can be applied to the plan and create a new predicate
        HashSet<Expression> applicableConjuncts = new HashSet<>();
        PredicateUtils.findExprsUsingSchemas(unusedConjuncts, false,
                applicableConjuncts, plan.getSchema());

        // Apply conjuncts to plan as a predicate
        Expression predicate = PredicateUtils.makePredicate(applicableConjuncts);
        boolean changed = false;
        if (predicate != null) {
            PlanUtils.addPredicateToPlan(plan, predicate);
            leafConjuncts.addAll(applicableConjuncts);
            changed = true;
        }

        // Apply alias if necessary
        if (fromClause.isRenamed()) {
            plan = new RenameNode(plan, fromClause.getResultName());
            changed = true;
        }

        // Update the plan's cost and statistics after any changes
        if (changed) {
            plan.prepare();
        }

        return plan;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans = new HashMap<>();

        // Initially populate joinPlans with just the N leaf plans.
        // At this point, leaf.leavesUsed should contain only leaf.joinPlan
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<>();

            for (Map.Entry<HashSet<PlanNode>, JoinComponent> entry : joinPlans.entrySet()) {
                HashSet<PlanNode> currUsed = entry.getKey();
                JoinComponent currBest = entry.getValue();

                for (JoinComponent leaf : leafComponents) {
                    // If leaf is already used, then continue
                    if (currUsed.contains(leaf.joinPlan))
                        continue;

                    // Get all conjuncts used by subplans
                    HashSet<Expression> nextConjunctsUsed = new HashSet<>(currBest.conjunctsUsed);
                    nextConjunctsUsed.addAll(leaf.conjunctsUsed);

                    // Apply unused and available conjuncts
                    HashSet<Expression> unusedConjuncts = new HashSet<>(conjuncts);
                    unusedConjuncts.removeAll(nextConjunctsUsed);
                    HashSet<Expression> applicableConjuncts = new HashSet<>();
                    PredicateUtils.findExprsUsingSchemas(
                            unusedConjuncts, false, applicableConjuncts,
                            currBest.joinPlan.getSchema(), leaf.joinPlan.getSchema());
                    Expression predicate = PredicateUtils.makePredicate(applicableConjuncts);
                    nextConjunctsUsed.addAll(applicableConjuncts);

                    // Next join plan, using INNER join
                    PlanNode nextPlan = new NestedLoopJoinNode(
                            currBest.joinPlan, leaf.joinPlan, JoinType.INNER, predicate);

                    // Next set of leaves used (KEY)
                    HashSet<PlanNode> nextUsed = new HashSet<>(currUsed);
                    nextUsed.add(leaf.joinPlan);

                    // See if new plan is better than old plan by CPU cost
                    nextPlan.prepare();
                    if (!nextJoinPlans.keySet().contains(nextUsed) ||
                        (nextPlan.getCost().cpuCost <
                         nextJoinPlans.get(nextUsed).joinPlan.getCost().cpuCost)) {

                        // Next JoinComponent (VALUE)
                        JoinComponent nextJoinComponent =
                                new JoinComponent(nextPlan, nextUsed, nextConjunctsUsed);

                        // Put in HashMap, overwriting previous value if exists
                        nextJoinPlans.put(nextUsed, nextJoinComponent);
                    }
                }
            }

            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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
