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

package org.voltdb.planner;

import java.io.File;
import java.io.IOException;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.ServerThread;
import org.voltdb.client.ProcCallException;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.ByteBuilder;
import org.voltdb.utils.BuildDirectoryUtils;

import edu.brown.hstore.HStoreConstants;
import junit.framework.TestCase;

public class TPCCDebugTest extends TestCase {
    protected Client client;
    protected ServerThread server;

    static final long W_ID = 3L;
    static final long W2_ID = 4L;
    static final long D_ID = 7L;
    static final long D2_ID = 8L;
    static final long O_ID = 9L;
    static final long C_ID = 42L;
    static final long I_ID = 12345L;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> ALL_PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        /*debugTPCCostat.class, debugTPCCpayment.class,*/ debugUpdateProc.class
        /*debugTPCCdelivery.class, debugTPCCslev.class*/
    };
    public static final Class<?>[] SUPPLEMENTALS = {
            ByteBuilder.class, TPCCConstants.class };

    static final String JAR = "tpcc.jar";

    @Override
    public void setUp() throws IOException {
        int siteCount = 1;
        BackendTarget target = BackendTarget.NATIVE_EE_JNI;
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + JAR;

        TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(ALL_PROCEDURES);
        pb.addSupplementalClasses(SUPPLEMENTALS);
        pb.compile(catalogJar, siteCount, 0);

        // start VoltDB server using hzsqlsb backend
        server = new ServerThread(catalogJar, target);
        server.start();
        server.waitForInitialization();

        client = ClientFactory.createClient();
        // connect
        client.createConnection(null, "localhost", HStoreConstants.DEFAULT_PORT, "program", "none");
    }

    public void waitUntilDone() throws InterruptedException {
        server.join();
    }

    @Override
    public void tearDown() throws InterruptedException {
        server.shutdown();
    }

    /*public void testOStatDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCostat", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    /*public void testPaymentDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCpayment", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    public void testUpdateDebug() throws IOException, ProcCallException {
        VoltTable[] retvals = client.callProcedure("debugUpdateProc", 0L).getResults();
        assertTrue(retvals.length == 0);
    }

    /*public void testDeliveryDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCdelivery", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    /*public void testSlevDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCslev", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
}
