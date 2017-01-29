package edu.caltech.nanodb.storage.heapfile;


import java.io.EOFException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.ColumnStatsCollector;

import edu.caltech.nanodb.queryeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.relations.SQLDataType;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;


/**
 * This class implements the TupleFile interface for heap files.
 */
public class HeapTupleFile implements TupleFile {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeapTupleFile.class);


    /**
     * The storage manager to use for reading and writing file pages, pinning
     * and unpinning pages, write-ahead logging, and so forth.
     */
    private StorageManager storageManager;


    /**
     * The manager for heap tuple files provides some higher-level operations
     * such as saving the metadata of a heap tuple file, so it's useful to
     * have a reference to it.
     */
    private HeapTupleFileManager heapFileManager;


    /** The schema of tuples in this tuple file. */
    private TableSchema schema;


    /** Statistics for this tuple file. */
    private TableStats stats;


    /** The file that stores the tuples. */
    private DBFile dbFile;


    public HeapTupleFile(StorageManager storageManager,
                         HeapTupleFileManager heapFileManager, DBFile dbFile,
                         TableSchema schema, TableStats stats) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (heapFileManager == null)
            throw new IllegalArgumentException("heapFileManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        this.storageManager = storageManager;
        this.heapFileManager = heapFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
    }


    @Override
    public TupleFileManager getManager() {
        return heapFileManager;
    }


    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public TableStats getStats() {
        return stats;
    }


    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if
     * there are no tuples in the file.
     */
    @Override
    public Tuple getFirstTuple() throws IOException {
        HeapFilePageTuple first = null;
        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.
page_scan:  // So we can break out of the outer loop from inside the inner one
            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page.
                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;

                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    first = new HeapFilePageTuple(schema, dbPage, iSlot, offset);
                    // Unpin current page, since loading the tuple pinned it again.
                    dbPage.unpin();
                    break page_scan;
                }
                // Unpin current page since we are done using it.
                dbPage.unpin();
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                         ".  Returning null.");
        }

        return first;
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by many other operations in the database, such as
     * indexes.
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     */
    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBPage dbPage;
        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                fptr.getPageNo() + " doesn't exist in file " +
                dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = DataPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = DataPage.getSlotValue(dbPage, slot);
        if (offset == DataPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot +
                " on page " + fptr.getPageNo() + " is empty.");
        }

        // Unpin current page, since loading the tuple will pin
        // it again.
        dbPage.unpin();

        return new HeapFilePageTuple(schema, dbPage, slot, offset);
    }


    /**
     * Returns the tuple that follows the specified tuple, or {@code null} if
     * there are no more tuples in the file.  This method must operate
     * correctly regardless of whether the input tuple is pinned or unpinned.
     *
     * @param tup the "previous tuple" that specifies where to start looking
     *        for the next tuple
     */
    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {

        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        // Retrieve the location info from the previous tuple.  Since the
        // tuple (and/or its backing page) may already have a pin-count of 0,
        // we can't necessarily use the page itself.
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage prevDBPage = ptup.getDBPage();
        DBFile dbFile = prevDBPage.getDBFile();
        int prevPageNo = prevDBPage.getPageNo();
        int prevSlot = ptup.getSlot();

        // Retrieve the page itself so that we can access the internal data.
        // The page will come back pinned on behalf of the caller.  (If the
        // page is still in the Buffer Manager's cache, it will not be read
        // from disk, so this won't be expensive in that case.)
        DBPage dbPage = storageManager.loadDBPage(dbFile, prevPageNo);

        HeapFilePageTuple nextTup = null;

        // Start by looking at the slot immediately following the previous
        // tuple's slot.
        int nextSlot = prevSlot + 1;

page_scan:  // So we can break out of the outer loop from inside the inner loop.
        while (true) {
            int numSlots = DataPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != DataPage.EMPTY_SLOT) {
                    // Creating this tuple will pin the page a second time.
                    nextTup = new HeapFilePageTuple(schema, dbPage, nextSlot,
                                                    nextOffset);
                    // Unpin current page, since loading the tuple pinned it again.
                    dbPage.unpin();
                    break page_scan;
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            dbPage.unpin(); // Unpin the current page.

            try {
                // Loading the next page pins it.
                dbPage = storageManager.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                break;
            }
        }

        return nextTup;
    }


    /**
     * Adds the specified tuple into the table file.  A new
     * <tt>HeapFilePageTuple</tt> object corresponding to the tuple is returned.
     *
     * @design (tongcharlie) A linked-list is used to keep track of pages with
     *         free space, ordered in ascending order by index. This kills the
     *         O(N^2) traversal time of the default implementation, and also
     *         greatly reduces the number of pages read during an insert.
     *
     *         Specifically, the header page (index = 0) and all data pages
     *         (index >= 1) have a "NextFreeBlock" field, which is simply an
     *         integer storing the index of the next node in the linked list. A
     *         value of 0 means the end of the list. Nodes that are not in the
     *         list also store a value of 0.
     *
     *         The interfaces for fetching this value are slightly different for
     *         the two pages:
     *         For the header page, use
     *         {@link HeaderPage#getNextFreeBlock(DBPage)} and
     *         {@link HeaderPage#setNextFreeBlock(DBPage, int)}.
     *         For data pages, use
     *         {@link DataPage.MetaData#getNextFreeBlock(DBPage)}} and
     *         {@link DataPage.MetaData#setNextFreeBlock(DBPage, int)}.
     *
     *         When a page is determined to not have enough space for new
     *         tuples, it is removed from the linked list. The criteria for
     *         removal is simply not having enough space to add the current
     *         tuple. This is not necessarily optimal -- there could be
     *         smaller tuples that may possibly fit -- but it is simple, has
     *         superior performance, and still keeps a reasonable file size.
     *
     *         If there are no suitable pages available, a new page is created
     *         and added to the linked list.
     *
     *         When tuples are deleted from a page, the page is readded to the
     *         linked list, since it should now have space for more tuples.
     *
     * @review (donnie) This could be made a little more space-efficient.
     *         Right now when computing the required space, we assume that we
     *         will <em>always</em> need a new slot entry, whereas the page may
     *         contain empty slots.  (Note that we don't always create a new
     *         slot when adding a tuple; we will reuse an empty slot.  This
     *         inefficiency is simply in estimating the size required for the
     *         new tuple.)
     */
    @Override
    public Tuple addTuple(Tuple tup) throws IOException {

        /*
         * Check to see whether any constraints are violated by
         * adding this tuple
         *
         * Find out how large the new tuple will be, so we can find a page to
         * store it.
         *
         * Find a page with space for the new tuple.
         *
         * Generate the data necessary for storing the tuple into the file.
         */

        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize +
                " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // Get the header page.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);

        // Get the next page with free space.
        int nextPageIndex = HeaderPage.getNextFreeBlock(headerPage);
        DBPage nextPage = null;

        // Evict pages out of the linked-list until one has enough space.
        while (nextPageIndex != 0) {
            nextPage = storageManager.loadDBPage(dbFile, nextPageIndex);

            int freeSpace = DataPage.getFreeSpaceInPage(nextPage);

            logger.trace(String.format("Page %d has %d bytes of free space.",
                    nextPageIndex, freeSpace));

            // If this page has enough free space to add a new tuple, break
            // out of the loop.  (The "+ 2" is for the new slot entry we will
            // also need.)
            if (freeSpace >= tupSize + 2) {
                logger.debug(
                    "Found space for new tuple in page " + nextPageIndex + ".");
                break;
            }

            // If we reached this point then the page doesn't have enough
            // space, so remove the page from the linked list and continue.
            logger.debug("Removed page " + nextPageIndex + " from linked-list");
            nextPageIndex = DataPage.MetaData.getNextFreeBlock(nextPage);
            DataPage.MetaData.setNextFreeBlock(nextPage, 0);
            DataPage.MetaData.setIsInLinkedList(nextPage, false);
            HeaderPage.setNextFreeBlock(headerPage, nextPageIndex);

            nextPage.unpin(); // unpin the evicted page (no longer needed).
        }

        // No more free pages, create new page.
        if (nextPageIndex == 0) {
            // Try to create a new page at the end of the file.
            nextPageIndex = dbFile.getNumPages();
            logger.debug(
                "Creating new page " + nextPageIndex + " to store new tuple.");
            nextPage = storageManager.loadDBPage(dbFile, nextPageIndex, true);
            DataPage.initNewPage(nextPage);

            // Add this page to the linked list.
            HeaderPage.setNextFreeBlock(headerPage, nextPageIndex);

            // Mark as active page in linked-list.
            DataPage.MetaData.setNextFreeBlock(nextPage, 0);
            DataPage.MetaData.setIsInLinkedList(nextPage, true);
        }

        // Insert the tuple into the page.
        int slot = DataPage.allocNewTuple(nextPage, tupSize);
        int tupOffset = DataPage.getSlotValue(nextPage, slot);

        logger.debug(String.format(
            "New tuple will reside on page %d, slot %d.", nextPageIndex, slot));

        HeapFilePageTuple pageTup =
            HeapFilePageTuple.storeNewTuple(schema, nextPage, slot, tupOffset, tup);

        DataPage.sanityCheck(nextPage);

        // We need to unpin the tuple given in the argument if it was part of
        // a INSERT ... SELECT statement.
        if (tup.isPinned()) {
            tup.unpin();
        }

        // At this point, only 2 pages are still pinned: the header page and
        // the page we inserted the tuple into. We unpin both pages and also
        // unpin the newly added tuple.
        headerPage.unpin();
        nextPage.unpin();

        return pageTup;
    }


    // Inherit interface-method documentation.
    /**
     * @review (donnie) This method will fail if a tuple is modified in a way
     *         that requires more space than is currently available in the data
     *         page.  One solution would be to move the tuple to a different
     *         page and then perform the update, but that would cause all kinds
     *         of additional issues.  So, if the page runs out of data, oh well.
     */
    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();

            int colIndex = schema.getColumnIndex(colName);
            ptup.setColumnValue(colIndex, value);
        }

        DBPage dbPage = ptup.getDBPage();
        DataPage.sanityCheck(dbPage);
    }


    // Inherit interface-method documentation.
    @Override
    public void deleteTuple(Tuple tup) throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        DataPage.deleteTuple(dbPage, ptup.getSlot());
        DataPage.sanityCheck(dbPage);

        // Note that we don't invalidate the page-tuple when it is deleted,
        // so that the tuple can still be unpinned, etc.

        // Since a tuple was deleted, we assume this page has space for new
        // tuples now. If this page is not in the linked-list already, add it
        // back to the linked-list.
        if (DataPage.MetaData.isInLinkedList(dbPage))
            return;

        int thisPageIndex = dbPage.getPageNo();

        // Set up for linked list traversal, starting with the header page.
        DBPage currPage = storageManager.loadDBPage(dbFile, 0);
        int nextPageIndex = HeaderPage.getNextFreeBlock(currPage);

        // Advance the linked-list until hitting the end, or this page's index.
        while (nextPageIndex != 0 && nextPageIndex < thisPageIndex) {
            currPage.unpin();   // Unpin current page before advancing.

            currPage = storageManager.loadDBPage(dbFile, nextPageIndex);
            nextPageIndex = DataPage.MetaData.getNextFreeBlock(currPage);
        }

        // Make sure the next page IS NOT this page, and insert.
        if (nextPageIndex != thisPageIndex) {
            logger.debug(String.format("Inserted page %d between %d and %d.",
                    thisPageIndex, currPage.getPageNo(), nextPageIndex));

            // If the current page is the header page, use a different call.
            if (currPage.getPageNo() == 0)
                HeaderPage.setNextFreeBlock(currPage, thisPageIndex);
            else
                DataPage.MetaData.setNextFreeBlock(currPage, thisPageIndex);

            DataPage.MetaData.setNextFreeBlock(dbPage, nextPageIndex);
            DataPage.MetaData.setIsInLinkedList(dbPage, true);
        } else {
            // Should not reach here, otherwise internal state is inconsistent.
            logger.warn(
                    "Page" + thisPageIndex +
                    " is in linked-list but is not flagged correctly.");
        }

        currPage.unpin();   // Unpin pages.
    }


    @Override
    public void analyze() throws IOException {
        int tuplesCount = 0;
        int tuplesSize = 0;
        // Subtract one for header page
        int pagesCount = dbFile.getNumPages() - 1;
        int columns = schema.numColumns();
        ArrayList<ColumnStatsCollector> cols = new ArrayList<ColumnStatsCollector>(columns);
        ArrayList<ColumnStats> columnStats = new ArrayList<ColumnStats>(columns);


        // Create ColumnStatsCollectors for each column
        for(int i = 0; i < columns; i ++) {
            SQLDataType colSQLType = schema.getColumnInfo(i).getType().getBaseType();
            cols.add(new ColumnStatsCollector(colSQLType));
        }

        // Loop through pages
        for (int i = 1; i <= pagesCount; i ++) {
            DBPage page = storageManager.loadDBPage(dbFile, i);
            int slots = DataPage.getNumSlots(page);
            // Loop through tuple slots
            for (int j = 0; j < slots; j ++) {
                int offset = DataPage.getSlotValue(page, j);
                // If not null (i.e. tuple exists)
                if (offset != 0) {
                    tuplesCount += 1;
                    HeapFilePageTuple tup = new HeapFilePageTuple(schema, page, j, offset);
                    // Loop through columns
                    for (int k = 0; k < columns; k ++) {
                        cols.get(k).addValue(tup.getColumnValue(k));
                    }
                }
            }
            // Find difference between start and end of tuple positions on page
            tuplesSize += DataPage.getTupleDataEnd(page) - DataPage.getTupleDataStart(page);
        }

        float avgSize = (float) tuplesSize / tuplesCount;
        for (int i = 0; i < cols.size(); i ++) {
            columnStats.add(cols.get(i).getColumnStats());
        }
        stats = new TableStats(pagesCount, tuplesCount, avgSize, columnStats);
        heapFileManager.saveMetadata(this);
    }


    @Override
    public List<String> verify() throws IOException {
        // TODO!
        // Right now we will just report that everything is fine.
        return new ArrayList<String>();
    }


    @Override
    public void optimize() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
