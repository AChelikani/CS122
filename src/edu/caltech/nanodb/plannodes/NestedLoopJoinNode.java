package edu.caltech.nanodb.plannodes;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;
import edu.caltech.nanodb.queryeval.SelectivityEstimator;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;

import java.util.ArrayList;


/**
 * This plan node implements a nested-loop join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(NestedLoopJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;

    /** A tuple with the same number of columns as rightTuple but all null */
    private Tuple rightNullPaddedTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;


    public NestedLoopJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopJoinNode) {
            NestedLoopJoinNode other = (NestedLoopJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loop plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoop[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopJoinNode node = (NestedLoopJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // RIGHT OUTER JOIN is the same as LEFT OUTER JOIN but swapped.
        if (joinType == JoinType.RIGHT_OUTER) {
            swap();
            joinType = JoinType.LEFT_OUTER;
        }

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        cost = null;

        // TODO:  Implement the rest [not sure what else is needed]
        rightNullPaddedTuple = new TupleLiteral(rightSchema.numColumns());
    }


    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        switch (joinType) {
            // In INNER or CROSS join, we just process all non-void tuples
            // and return them if they can be joined.
            case INNER:
            case CROSS: {
                while (getTuplesToJoin()) {
                    if (rightTuple != null && canJoinTuples()) {
                        return joinTuples(leftTuple, rightTuple);
                    }
                }
                break;
            }

            // In LEFT_OUTER join, we need to include left tuples that do not
            // match any right tuples. To do this we check when the rightTuple
            // becomes null: this signals the end of the right tuple stream.
            // If the current leftTuple is the leftTuple we started with, then
            // we do *not* return it (because if we started on it, then that
            // means it matched previously). If it is *not* the leftTuple we
            // started on, then we can return it (padded with null) because
            // we traversed the entire right stream without a match.
            case LEFT_OUTER: {
                Tuple prevLeftTuple = leftTuple;
                while (getTuplesToJoin()) {
                    if (rightTuple == null) {
                        if (leftTuple != prevLeftTuple) {
                            return joinTuples(leftTuple, rightNullPaddedTuple);
                        }
                    }
                    else if (canJoinTuples()) {
                        return joinTuples(leftTuple, rightTuple);
                    }
                }
                break;
            }

            // In SEMIJOIN, return all left tuples that have a match. Before
            // traversing, we set rightTuple = null in order to restart the
            // right stream and also advance the left stream once.
            case SEMIJOIN: {
                rightTuple = null;
                while (getTuplesToJoin()) {
                    if (rightTuple != null && canJoinTuples()) {
                        return leftTuple;
                    }
                }
                break;
            }

            // In ANTIJOIN, return all left tuples that do not match. Again we
            // use the rightTuple == null comparison to determine the end of
            // the right stream. If any right tuple matches, then we skip to
            // the end of the stream by setting rightTuple = null. This
            // restarts the right stream and also advances the left stream once.
            case ANTIJOIN: {
                while (getTuplesToJoin()) {
                    if (rightTuple == null) {
                        return leftTuple;
                    }
                    else if (canJoinTuples()) {
                        rightTuple = null;
                    }
                }
                break;
            }

            // This probably should not be reached, because joinType should be
            // already filtered on what is supported. In any case, this is an
            // additional precaution for possible bugs.
            default: {
                logger.warn("Trying to process JoinType of " + joinType.name());
                return null;
            }
        }

        done = true;
        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loop logic.
     *
     * This function will set rightTuple = null at the end of the right stream.
     *
     * Ex. if the left stream is (L1, L2) and the right stream is
     * (R1, R2), then subsequent calls to this method will yield:
     *
     * {leftTuple, rightTuple}:
     * {L1, R1},
     * {L1, R2},
     * {L1, null},
     * {L2, R1},
     * {L2, R2},
     * {L2, null},
     * {null, R1}
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        // If right tuple is null, reset and advance left tuple by 1.
        if (rightTuple == null) {
            if (leftTuple != null) {
                leftTuple.unpin();
            }
            leftTuple = leftChild.getNextTuple();
            if (leftTuple != null) {
                rightChild.initialize();
                rightTuple = rightChild.getNextTuple();
            }
        }

        // Otherwise, advance right tuple.
        else {
            rightTuple.unpin();
            rightTuple = rightChild.getNextTuple();
        }

        // Left tuple must be non-null to signify streams still have content.
        return leftTuple != null;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
