package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with some <tt>NATURAL JOINS</tt>.
 */
@Test
public class TestNaturalJoins extends SqlTestCase {

    public TestNaturalJoins() {
        super("setup_testNaturalJoins");
    }


    /**
     * This test performs <tt>NATURAL JOIN</tt> on two matching columns.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testNaturalJoin() throws Throwable {
        String query1 =
                "SELECT * FROM " +
                    "test_natural_joins_left_empty  AS t1 " +
                        "NATURAL JOIN " +
                    "test_natural_joins_right       AS t2 ";
        String query2 =
                "SELECT * FROM " +
                    "test_natural_joins_left        AS t1 " +
                        "NATURAL JOIN " +
                    "test_natural_joins_right_empty AS t2 ";
        String query3 =
                "SELECT * FROM " +
                    "test_natural_joins_left_empty  AS t1 " +
                        "NATURAL JOIN " +
                    "test_natural_joins_right_empty AS t2 ";
        String query4 =
                "SELECT * FROM " +
                    "test_natural_joins_left  AS t1 " +
                        "NATURAL JOIN " +
                    "test_natural_joins_right AS t2 ";

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
