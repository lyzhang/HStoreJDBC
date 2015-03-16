/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import java.io.IOException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.Hstoreservice.Status;

public class TestSystemProcedureSuite extends RegressionSuite {

    public TestSystemProcedureSuite(String name) {
        super(name);
    }

    public void testInvalidProcedureName() throws IOException {
        Client client = getClient();
        try {
            client.callProcedure("@SomeInvalidSysProcName", "1", "2");
        }
        catch (Exception e2) {
            assertTrue(e2.getMessage(), e2.getMessage().toLowerCase().contains("unknown"));
            return;
        }
        fail("Expected exception.");
    }

    public void testStatistics_Table() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        
        int num_tables = getCatalogContext().getDataTables().size();
        int num_partitions = getCatalogContext().numberOfPartitions;
        
        //System.err.println("Row Count: " + results[0].getRowCount());
        //System.err.println("# of Tables: " + num_tables);
        //System.err.println("# of Partitions: " + num_partitions);
        
        assertEquals(results[0].getRowCount(), num_partitions * num_tables);
        //System.out.println("Test statistics table: " + results[0].toString());
    }

    public void testStatistics_IndexStats() throws Exception {
        Client client = getClient();
        System.err.println("Status name: " + SysProcSelector.INDEX.name());
        final VoltTable results[] = client.callProcedure("@Statistics", "index", 0).getResults();
        assertEquals( 1, results.length);
        assertTrue( results[0] != null);
        System.err.println(VoltTableUtil.format(results[0]));
    }

//    public void testStatistics_Procedure() throws Exception {
//        Client client  = getClient();
//        VoltTable results[] = null;
//        results = client.callProcedure("@Statistics", "procedure", 0).getResults();
//        // one aggregate table returned
//        assertTrue(results.length == 1);
//        System.out.println("Test procedures table: " + results[0].toString());
//    }

    public void testStatistics_iostats() throws Exception {
        Client client  = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@Statistics", "iostats", 0).getResults();
        // one aggregate table returned
        assertTrue(results.length == 1);
        System.out.println("Test iostats table: " + results[0].toString());
    }

    public void testStatistics_InvalidSelector() throws IOException {
        Client client = getClient();
        boolean exceptionThrown = false;

        // No selector at all.
        try {
            client.callProcedure("@Statistics");
        }
        catch (Exception ex) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;

        // Invalid selector
        try {
            client.callProcedure("@Statistics", "garbage", 0);
        }
        catch (Exception ex) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    public void testStatistics_PartitionCount() throws Exception {
        Client client = getClient();
        final VoltTable results[] =
            client.callProcedure("@Statistics", SysProcSelector.PARTITIONCOUNT.name(), 0).getResults();
        assertEquals( 1, results.length);
        assertTrue( results[0] != null);
        assertEquals( 1, results[0].getRowCount());
        assertEquals( 1, results[0].getColumnCount());
        assertEquals( VoltType.INTEGER, results[0].getColumnType(0));
        assertTrue( results[0].advanceRow());
        final int columnCount = (int)results[0].getLong(0);
        assertTrue (columnCount == 2 || columnCount == 4);
    }

    //public void testShutdown() {
    //    running @shutdown kills the JVM.
    //    not sure how to test this.
    // }

//    public void testSystemInformation() throws IOException, ProcCallException {
//        Client client = getClient();
//        VoltTable results[] = client.callProcedure("@SystemInformation").getResults();
//        assertEquals(1, results.length);
//        System.out.println(results[0]);
//    }

    // Pretty lame test but at least invoke the procedure.
    // "@Quiesce" is used more meaningfully in TestELTSuite.
    public void testQuiesce() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@Quiesce").getResults();
        assertEquals(1, results.length);
        System.err.println(VoltTableUtil.format(results[0]));
        assertEquals(results[0].getRowCount(), this.getServerConfig().getPartitionCount());
        while (results[0].advanceRow()) {
            String status = results[0].getString("STATUS");
            assertEquals(Status.OK.toString(), status);    
        } // WHILE
    }

    public void testLoadMulipartitionTable_InvalidTableName() throws IOException, ProcCallException {
        Client client = getClient();
        try {
            client.callProcedure("@LoadMultipartitionTable", "DOES_NOT_EXIST", null, 1);
        } catch (ProcCallException ex) {
            assertTrue(true);
            return;
        }
        fail();
    }

    public void testLoadMultipartitionTable() throws IOException {
        Client client = getClient();
        // make a TPCC warehouse table
        VoltTable partitioned_table = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", org.voltdb.VoltType.SMALLINT),
                new VoltTable.ColumnInfo("W_NAME", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_1", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_2", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_CITY", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STATE", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_ZIP", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_TAX",org.voltdb.VoltType.get((byte)8)),
                new VoltTable.ColumnInfo("W_YTD", org.voltdb.VoltType.get((byte)8))
        );

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {new Short((short) i),
                                         "name_" + i,
                                         "street1_" + i,
                                         "street2_" + i,
                                         "city_" + i,
                                         "ma",
                                         "zip_"  + i,
                                         new Double(i),
                                         new Double(i)};
            partitioned_table.addRow(row);
        }

        // make a TPCC item table
        VoltTable replicated_table =
            new VoltTable(new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                          new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                          new VoltTable.ColumnInfo("I_DATA", VoltType.STRING));

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {i,
                                         i,
                                         "name_" + i,
                                         new Double(i),
                                         "data_"  + i};

            replicated_table.addRow(row);
        }

        try {
            client.callProcedure("@LoadMultipartitionTable", "WAREHOUSE", partitioned_table);
            client.callProcedure("@LoadMultipartitionTable", "ITEM", replicated_table);
            VoltTable results[] = client.callProcedure("@Statistics", "table", 0).getResults();

            int foundItem = 0;
            // to verify, each of the 2 sites should have 5 warehouses.
            int foundWarehouse = 0;

            System.out.println(results[0]);

            // Check that tables loaded correctly
            while(results[0].advanceRow()) {
                if (results[0].getString("TABLE_NAME").equals("WAREHOUSE")) {
                    ++foundWarehouse;
                    //Different values depending on local cluster vs. single process hence ||
                    assertTrue(5 == results[0].getLong("TUPLE_COUNT") ||
                            10 == results[0].getLong("TUPLE_COUNT"));
                }
                if (results[0].getString("TABLE_NAME").equals("ITEM"))
                {
                    ++foundItem;
                    //Different values depending on local cluster vs. single process hence ||
                    assertTrue(10 == results[0].getLong("TUPLE_COUNT") ||
                            20 == results[0].getLong("TUPLE_COUNT"));
                }
            }
            // make sure both warehouses were located
            //Different values depending on local cluster vs. single process hence ||
            assertTrue(2 == foundWarehouse || 4 == foundWarehouse);
            assertTrue(2 == foundItem || 4 == foundItem);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple backends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSystemProcedureSuite.class);

        // Not really using TPCC functionality but need a database.
        // The testLoadMultipartitionTable procedure assumes partitioning
        // on warehouse id.
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        config = new LocalSingleProcessServer("sysproc-twosites.jar", 2,
                                              BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);


        /*
         * Add a cluster configuration for sysprocs too
         */
        config = new LocalCluster("sysproc-cluster.jar", 2, 2, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }
}


