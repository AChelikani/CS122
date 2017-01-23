package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with some simple <tt>JOINS</tt>.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }


    /**
     * This test performs simple <tt>INNER JOIN</tt> statements.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoin() throws Throwable {
        String query1 =
                "SELECT * FROM " +
                    "test_simple_joins_left_empty AS t1 " +
                        "INNER JOIN " +
                    "test_simple_joins_right      AS t2 " +
                "ON t1.a = t2.a";
        String query2 =
                "SELECT * FROM " +
                    "test_simple_joins_left        AS t1 " +
                        "INNER JOIN " +
                    "test_simple_joins_right_empty AS t2 " +
                "ON t1.a = t2.a";
        String query3 =
                "SELECT * FROM " +
                    "test_simple_joins_left_empty  AS t1 " +
                        "INNER JOIN " +
                    "test_simple_joins_right_empty AS t2 " +
                "ON t1.a = t2.a";
        String query4 =
                "SELECT * FROM " +
                    "test_simple_joins_left  AS t1 " +
                        "INNER JOIN " +
                    "test_simple_joins_right AS t2 " +
                "ON t1.a = t2.a";

        TupleLiteral[] empty = {};
        TupleLiteral[] expected4 = {
                new TupleLiteral(1, 10, 1, "dat"),
                new TupleLiteral(1, 15, 1, "dat"),
                new TupleLiteral(2, 20, 2, "boy"),
                new TupleLiteral(2, 20, 2, "oh"),
                new TupleLiteral(4, 40, 4, null),
                new TupleLiteral(4, null, 4, null),
                new TupleLiteral(4, 40, 4, "waddup"),
                new TupleLiteral(4, null, 4, "waddup"),
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
     * This test performs simple <tt>LEFT OUTER JOIN</tt> statements.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftOuterJoin() throws Throwable {
        String query1 =
                "SELECT * FROM " +
                    "test_simple_joins_left_empty AS t1 " +
                        "LEFT OUTER JOIN " +
                    "test_simple_joins_right      AS t2 " +
                "ON t1.a = t2.a";
        String query2 =
                "SELECT * FROM " +
                    "test_simple_joins_left        AS t1 " +
                        "LEFT OUTER JOIN " +
                    "test_simple_joins_right_empty AS t2 " +
                "ON t1.a = t2.a";
        String query3 =
                "SELECT * FROM " +
                    "test_simple_joins_left_empty  AS t1 " +
                        "LEFT OUTER JOIN " +
                    "test_simple_joins_right_empty AS t2 " +
                "ON t1.a = t2.a";
        String query4 =
                "SELECT * FROM " +
                    "test_simple_joins_left  AS t1 " +
                        "LEFT OUTER JOIN " +
                    "test_simple_joins_right AS t2 " +
                "ON t1.a = t2.a";

        TupleLiteral[] empty = {};
        TupleLiteral[] expected2 = {
                new TupleLiteral(0, null, null, null),
                new TupleLiteral(1, 10, null, null),
                new TupleLiteral(1, 15, null, null),
                new TupleLiteral(2, 20, null, null),
                new TupleLiteral(3, 30, null, null),
                new TupleLiteral(4, 40, null, null),
                new TupleLiteral(4, null, null, null),
        };
        TupleLiteral[] expected4 = {
                new TupleLiteral(0, null, null, null),
                new TupleLiteral(3, 30, null, null),
                new TupleLiteral(1, 10, 1, "dat"),
                new TupleLiteral(1, 15, 1, "dat"),
                new TupleLiteral(2, 20, 2, "boy"),
                new TupleLiteral(2, 20, 2, "oh"),
                new TupleLiteral(4, 40, 4, null),
                new TupleLiteral(4, null, 4, null),
                new TupleLiteral(4, 40, 4, "waddup"),
                new TupleLiteral(4, null, 4, "waddup"),
        };

        CommandResult result1 = server.doCommand(query1, true);
        assert checkUnorderedResults(empty, result1);

        CommandResult result2 = server.doCommand(query2, true);
        assert checkUnorderedResults(expected2, result2);

        CommandResult result3 = server.doCommand(query3, true);
        assert checkUnorderedResults(empty, result3);

        CommandResult result4 = server.doCommand(query4, true);
        assert checkUnorderedResults(expected4, result4);
    }



    /**
     * This test performs simple <tt>RIGHT OUTER JOIN</tt> statements.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightOuterJoin() throws Throwable {
        String query1 =
                "SELECT * FROM " +
                    "test_simple_joins_left_empty AS t1 " +
                        "RIGHT OUTER JOIN " +
                    "test_simple_joins_right      AS t2 " +
                "ON t1.a = t2.a";
        String query2 =
                "SELECT * FROM " +
                    "test_simple_joins_left        AS t1 " +
                        "RIGHT OUTER JOIN " +
                    "test_simple_joins_right_empty AS t2 " +
                "ON t1.a = t2.a";
        String query3 =
                "SELECT * FROM " +
                    "test_simple_joins_left_empty  AS t1 " +
                        "RIGHT OUTER JOIN " +
                    "test_simple_joins_right_empty AS t2 " +
                "ON t1.a = t2.a";
        String query4 =
                "SELECT * FROM " +
                    "test_simple_joins_left  AS t1 " +
                        "RIGHT OUTER JOIN " +
                    "test_simple_joins_right AS t2 " +
                "ON t1.a = t2.a";

        TupleLiteral[] empty = {};
        TupleLiteral[] expected1 = {
                new TupleLiteral(null, null, 8, "its"),
                new TupleLiteral(null, null, 1, "dat"),
                new TupleLiteral(null, null, 2, "boy"),
                new TupleLiteral(null, null, 2, "oh"),
                new TupleLiteral(null, null, 4, null),
                new TupleLiteral(null, null, 4, "waddup"),
                new TupleLiteral(null, null, 5, "!!!"),
        };
        TupleLiteral[] expected4 = {
                new TupleLiteral(null, null, 8, "its"),
                new TupleLiteral(null, null, 5, "!!!"),
                new TupleLiteral(1, 10, 1, "dat"),
                new TupleLiteral(1, 15, 1, "dat"),
                new TupleLiteral(2, 20, 2, "boy"),
                new TupleLiteral(2, 20, 2, "oh"),
                new TupleLiteral(4, 40, 4, null),
                new TupleLiteral(4, null, 4, null),
                new TupleLiteral(4, 40, 4, "waddup"),
                new TupleLiteral(4, null, 4, "waddup"),
        };

        CommandResult result1 = server.doCommand(query1, true);
        System.out.println(result1);
        assert checkUnorderedResults(expected1, result1);

        CommandResult result2 = server.doCommand(query2, true);
        assert checkUnorderedResults(empty, result2);

        CommandResult result3 = server.doCommand(query3, true);
        assert checkUnorderedResults(empty, result3);

        CommandResult result4 = server.doCommand(query4, true);
        assert checkUnorderedResults(expected4, result4);
    }
}
