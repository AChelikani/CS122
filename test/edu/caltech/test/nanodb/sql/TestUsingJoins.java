package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with some <tt>USING JOINS</tt>.
 */
@Test
public class TestUsingJoins extends SqlTestCase {

    public TestUsingJoins() {
        super("setup_testUsingJoins");
    }


    /**
     * This test performs <tt>JOIN</tt> using one column.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testUsingJoinSingleColumn() throws Throwable {
        String query1 =
                "SELECT * FROM " +
                    "test_using_joins_left_empty  AS t1 " +
                        "JOIN " +
                    "test_using_joins_right       AS t2 " +
                "USING (a)";
        String query2 =
                "SELECT * FROM " +
                    "test_using_joins_left        AS t1 " +
                        "JOIN " +
                    "test_using_joins_right_empty AS t2 " +
                "USING (a)";
        String query3 =
                "SELECT * FROM " +
                    "test_using_joins_left_empty  AS t1 " +
                        "JOIN " +
                    "test_using_joins_right_empty AS t2 " +
                "USING (a)";
        String query4 =
                "SELECT * FROM " +
                    "test_using_joins_left  AS t1 " +
                        "JOIN " +
                    "test_using_joins_right AS t2 " +
                "USING (a)";

        TupleLiteral[] empty = {};
        TupleLiteral[] expected4 = {
                new TupleLiteral(1, "hello", 10, "hello", "dat"),
                new TupleLiteral(1, "world", 15, "hello", "dat"),
                new TupleLiteral(2, "lol", 20, "lol", "boy"),
                new TupleLiteral(2, "lol", 20, "wat", "oh"),
                new TupleLiteral(4, null, 40, null, null),
                new TupleLiteral(4, null, 40, "match", "waddup"),
                new TupleLiteral(4, "match", null, null, null),
                new TupleLiteral(4, "match", null, "match", "waddup"),
        };

        CommandResult result1 = server.doCommand(query1, true);
        assert checkUnorderedResults(empty, result1);

        CommandResult result2 = server.doCommand(query2, true);
        assert checkUnorderedResults(empty, result2);

        CommandResult result3 = server.doCommand(query3, true);
        assert checkUnorderedResults(empty, result3);

        CommandResult result4 = server.doCommand(query4, true);
        assert checkUnorderedResults(expected4, result4);
    }


    /**
     * This test performs <tt>JOIN</tt> using two columns.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testUsingJoinDoubleColumn() throws Throwable {
        String query1 =
                "SELECT * FROM " +
                    "test_using_joins_left_empty  AS t1 " +
                        "JOIN " +
                    "test_using_joins_right       AS t2 " +
                "USING (a, b)";
        String query2 =
                "SELECT * FROM " +
                    "test_using_joins_left        AS t1 " +
                        "JOIN " +
                    "test_using_joins_right_empty AS t2 " +
                "USING (a, b)";
        String query3 =
                "SELECT * FROM " +
                    "test_using_joins_left_empty  AS t1 " +
                        "JOIN " +
                    "test_using_joins_right_empty AS t2 " +
                "USING (a, b)";
        String query4 =
                "SELECT * FROM " +
                    "test_using_joins_left  AS t1 " +
                        "JOIN " +
                    "test_using_joins_right AS t2 " +
                "USING (a, b)";

        TupleLiteral[] empty = {};
        TupleLiteral[] expected4 = {
                new TupleLiteral(1, "hello", 10, "dat"),
                new TupleLiteral(2, "lol", 20, "boy"),
                new TupleLiteral(4, "match", null, "waddup"),
        };

        CommandResult result1 = server.doCommand(query1, true);
        assert checkUnorderedResults(empty, result1);

        CommandResult result2 = server.doCommand(query2, true);
        assert checkUnorderedResults(empty, result2);

        CommandResult result3 = server.doCommand(query3, true);
        assert checkUnorderedResults(empty, result3);

        CommandResult result4 = server.doCommand(query4, true);
        assert checkUnorderedResults(expected4, result4);
    }
}
