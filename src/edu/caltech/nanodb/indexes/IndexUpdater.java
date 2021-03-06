package edu.caltech.nanodb.indexes;


import java.io.IOException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexUtils;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.EventDispatchException;
import edu.caltech.nanodb.server.RowEventListener;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class implements the {@link RowEventListener} interface to make sure
 * that all indexes on an updated table are kept up-to-date.  This handler is
 * installed by the {@link StorageManager#initialize} setup method.
 */
public class IndexUpdater implements RowEventListener {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexUpdater.class);


    /**
     * A cached reference to the index manager since we use it a lot in this
     * class.
     */
    private IndexManager indexManager;


    public IndexUpdater(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.indexManager = storageManager.getIndexManager();
    }


    @Override
    public void beforeRowInserted(TableInfo tblFileInfo, Tuple newValues) {
        // Ignore.
    }


    @Override
    public void afterRowInserted(TableInfo tblFileInfo, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowUpdated(TableInfo tblFileInfo, Tuple oldTuple,
                                 Tuple newValues) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowUpdated(TableInfo tblFileInfo, Tuple oldValues,
                                Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowDeleted(TableInfo tblFileInfo, Tuple oldTuple) {
        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowDeleted(TableInfo tblFileInfo, Tuple oldValues) {
        // Ignore.
    }


    /**
     * This helper method handles the case when a tuple is being added to the
     * table, after the row has already been added to the table.  All indexes
     * on the table are updated to include the new row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the new tuple that was inserted into the table
     */
    private void addRowToIndexes(TableInfo tblFileInfo, PageTuple ptup) {
        logger.debug("Adding tuple " + ptup + " to indexes for table " +
            tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnRefs indexDef : schema.getIndexes().values()) {
            try {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                // If the index is a unique index, then verify that there
                // isn't already a tuple in the index with the same values
                // (excluding the tuple-pointer column, of course).
                //
                // Finally, add a new tuple to the index, including the
                // tuple-pointer to the tuple in the table.
                // Helpful: IndexUtils.makeTableSearchKey()
                // Helpful: IndexUtils.findTupleIndex()

                // Make sure to not violate unique constraints
                // Check for existence of tuple
                if (indexDef.getConstraintType().isUnique()) {
                    // False arg since we we don't need to check pointer equivalence
                    TupleLiteral checkTup = IndexUtils.makeTableSearchKey(indexDef, ptup, false);
                    // Check for tuple with same values
                    if (IndexUtils.findTupleInIndex(checkTup, indexInfo.getTupleFile()) != null) {
                        // Tuple already exists
                        throw new IllegalArgumentException("Tuple already exists!");
                    }
                }

                // Create and add to index
                TupleLiteral tup = IndexUtils.makeTableSearchKey(indexDef, ptup, true);
                indexInfo.getTupleFile().addTuple(tup);

            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName(), e);
            }
        }
    }


    /**
     * This helper method handles the case when a tuple is being removed from
     * the table, before the row has actually been removed from the table.
     * All indexes on the table are updated to remove the row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the tuple about to be removed from the table
     */
    private void removeRowFromIndexes(TableInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Removing tuple " + ptup + " from indexes for table " +
            tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnRefs indexDef : schema.getIndexes().values()) {
            try {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                // Find and remove the entry in this index, corresponding to
                // the passed-in tuple.
                //
                // If the tuple doesn't appear in this index, throw an
                // IllegalStateException to indicate that the index is bad.
                TupleLiteral tup = IndexUtils.makeTableSearchKey(indexDef, ptup, true);
                PageTuple delTup = IndexUtils.findTupleInIndex(tup, indexInfo.getTupleFile());

                // No tuple to delete so throw error
                if (delTup == null) {
                    throw new IllegalStateException("Index " + indexDef.getIndexName() +
                            " in table " + tblFileInfo.getTableName() + " does not contain tuple");
                }

                // Otherwise remove tuple
                indexInfo.getTupleFile().deleteTuple(delTup);

            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName());
            }
        }
    }
}
