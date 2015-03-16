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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;

import edu.brown.hstore.conf.HStoreConf;

/**
 * Interface allowing for the use of a particular configuration/topology
 * of a VoltDB server. For example, an implementation of this class might
 * allow a user to manipulate a 20-node VoltDB cluster in the server room,
 * a 100 node VoltDB cluster in the cloud, or a single process VoltDB
 * instance running on the local machine. This class is given to tests which
 * run generically on top of a VoltServerConfig.
 *
 */
public abstract class VoltServerConfig {

    /**
     * Build a catalog jar with the required topology according to the
     * configuration parameters of the given VoltProjectBuilder instance.
     *
     * @param builder The VoltProjectBuilder instance describing the project to build.
     */
    public abstract boolean compile(VoltProjectBuilder builder);

    /**
     * Start the instance of VoltDB.
     */
    public abstract void startUp();

    /**
     * Shutdown the instance of VoltDB.
     * @returns A list of errors generated by Valgrind IPC or null
     *          if there were no errors or Valgrind was not running
     */
    public abstract List<String> shutDown() throws InterruptedException;

    /**
     * Get the list of hostnames/ips that are listening
     * for the running VoltDB instance.
     *
     * @return A list of hostnames/ips as strings.
     */
    public abstract List<String> getListenerAddresses();

    /**
     * Get the name of this particular configuration. This may be
     * combined with the test name to identify a combination of test
     * and server config to JUnit.
     *
     * @return The name of this config.
     */
    public abstract String getName();

    /**
     * Get the number of nodes running in this test suite
     */
    public abstract int getNodeCount();

    /**
     * Get the number of partitions running in this test suite
     */
    public abstract int getPartitionCount();

    /**
     * Get the CatalogContext used to deploy the cluster in this test suite
     */
    public abstract CatalogContext getCatalogContext();
    
    /**
     * Get the catalog used to deploy the cluster in this test suite
     */
    public abstract Catalog getCatalog();
    
    /**
     * @return Is the underlying instance of VoltDB running HSQL?
     */
    public abstract boolean isHSQL();

    /**
     * @return Is the underlying instance of VoltDB running IPC with Valgrind?
     */
    public abstract boolean isValgrind();
    
    // ---------------------------------------------------------------------------

    protected final Map<String, String> confParams = new HashMap<String, String>();
    protected String nameSuffix = "";
    
    public final void setTestNameSuffix(String suffix) {
        this.nameSuffix = suffix;
    }
 
    /**
     * Set an HStoreConf parameter to use when deploying the HStoreSites in the
     * regression tests.
     * @param name
     * @param value
     */
    public final void setConfParameter(String name, Object value) {
        assert(HStoreConf.isConfParameter(name)) :
            "Invalid HStoreConf parameter '" + name + "'";
        this.confParams.put(name, (value != null ? value.toString() : null));
        HStoreConf.singleton(true).set(name, value);
    }
    
    // ---------------------------------------------------------------------------
    
    public static String getPathToCatalogForTest(String jarname) {
        String answer = jarname;
        if (System.getenv("TEST_DIR") != null) {
            answer = System.getenv("TEST_DIR") + File.separator + jarname;
        }
        return answer;
    }
}
