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

import junit.framework.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltProcedure;
import org.voltdb.client.Client;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.UpdateNewOrder;
import org.voltdb.regressionsuites.multipartitionprocs.*;

import java.io.IOException;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestMultiPartitionSuite extends RegressionSuite {

    private static final String PREFIX = "multip";
    
    // procedures used by these tests
    @SuppressWarnings("unchecked")
    static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        MultiSiteSelect.class,
        MultiSiteIndexSelect.class,
        MultiSiteDelete.class,
        UpdateNewOrder.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestMultiPartitionSuite(String name) {
        super(name);
    }

    public void testSimpleScan() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 5L);

            System.out.println("\nBEGIN TEST\n==================\n");

            VoltTable[] results = client.callProcedure("MultiSiteSelect").getResults();

            assertTrue(results.length == 1);
            VoltTable resultAll = results[0];

            System.out.println("All Got " + String.valueOf(resultAll.getRowCount()) + " rows.");
            assertTrue(resultAll.getRowCount() == 4);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }

    public void testIndexScan() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 4L);

            System.out.println("\nBEGIN TEST\n==================\n");

            VoltTable[] results = client.callProcedure("MultiSiteIndexSelect").getResults();

            assertTrue(results.length == 2);

            System.out.println("All Got " + String.valueOf(results[0].getRowCount()) + " rows.");
            System.out.println("Index: " + results[0].toString());
            assertTrue(results[0].getRowCount() == 4);

            System.out.println("Index2 Got " + String.valueOf(results[1].getRowCount()) + " rows.");
            System.out.println("Index2: " + results[1].toString());
            assertTrue(results[1].getRowCount() == 1);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }

    public void testDelete() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 4L);

            System.out.println("\nBEGIN TEST\n==================\n");

            // delete a tuple
            VoltTable[] results = client.callProcedure("MultiSiteDelete").getResults();
            assertTrue(results.length == 1);
            VoltTable resultModCount = results[0];
            long modCount = resultModCount.asScalarLong();
            assertTrue(modCount == 1);

            // check for three remaining tuples
            results = client.callProcedure("MultiSiteSelect").getResults();
            assertTrue(results.length == 1);
            VoltTable allData = results[0];
            System.out.println("Leftover: " + allData.toString());
            assertTrue(allData.getRowCount() == 3);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }


    public void testUpdate() throws IOException {
        Client client = getClient();

        try {
            // parameters to InsertNewOrder are order, district, warehouse
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);

            // parameters to UpdateNewOrder are no_o_id, alwaysFail
            VoltTable[] results = client.callProcedure("UpdateNewOrder", 1L, 1L).getResults();
            assertTrue(results.length == 1);
            assertTrue(results[0].asScalarLong() == 1);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        try {
            // will always fail.
            client.callProcedure("UpdateNewOrder", 1L, 0L);
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
        }
    }


    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMultiPartitionSuite.class);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);


        return builder;
    }
}
