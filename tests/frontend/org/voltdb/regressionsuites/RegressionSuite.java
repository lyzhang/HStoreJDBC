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
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ConnectionUtil;

import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;

/**
 * Base class for a set of JUnit tests that perform regression tests
 * on a running VoltDB server. It is assumed that all tests will access
 * a particular VoltDB server to do their work and check the output of
 * any procedures called. The main feature of this class is that the
 * backend instance of VoltDB is very flexible and the tests can be run
 * on multiple instances of VoltDB to test different VoltDB topologies.
 *
 */
public class RegressionSuite extends TestCase {

    static {
        // log4j Hack
        LoggerUtil.setupLogging();
    }
    
    VoltServerConfig m_config;
    protected String m_username = "default";
    protected String m_password = "password";
    private final ArrayList<Client> m_clients = new ArrayList<Client>();
    private final ArrayList<SocketChannel> m_clientChannels = new ArrayList<SocketChannel>();
    private final DefaultRandomGenerator random = new DefaultRandomGenerator();

    /**
     * Trivial constructor that passes parameter on to superclass.
     * @param name The name of the method to run as a test. (JUnit magic)
     */
    public RegressionSuite(final String name) {
        super(name);
    }

    /**
     * JUnit special method called to setup the test. This instance will start
     * the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void setUp() {
        LoggerUtil.setupLogging();
        m_config.startUp();
    }

    /**
     * JUnit special method called to shutdown the test. This instance will
     * stop the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void tearDown() throws InterruptedException {
        final List<String> valgrindErrors = m_config.shutDown();
        if (valgrindErrors != null) {
            String failString = "";
            for (final String error : valgrindErrors) {
                failString = failString + error;
            }
            fail(failString);
        }
        for (final Client c : m_clients) {
            c.close();
        }
        synchronized (m_clientChannels) {
            for (final SocketChannel sc : m_clientChannels) {
                try {
                    ConnectionUtil.closeConnection(sc);
                } catch (final IOException e) {
                    e.printStackTrace();
                }
            }
            m_clientChannels.clear();
        }
        m_clients.clear();
        System.gc();
    }

    /**
     * @return Is the underlying instance of VoltDB running HSQL?
     */
    public boolean isHSQL() {
        return m_config.isHSQL();
    }

    /**
     * @return Is the underlying instance of VoltDB running Valgrind with the IPC client?
     */
    public boolean isValgrind() {
        return m_config.isValgrind();
    }

    /**
     * @return a reference to the associated VoltServerConfig
     */
    public final VoltServerConfig getServerConfig() {
        return m_config;
    }
    
    /**
     * @return The catalog used in the internal cluster configuration
     */
    @Deprecated
    public final Catalog getCatalog() {
        return m_config.getCatalog();
    }
    
    public final CatalogContext getCatalogContext() {
        return m_config.getCatalogContext();
    }

    public final AbstractRandomGenerator getRandom() {
        return (this.random);
    }
    
    /**
     * Get a VoltClient instance connected to the server driven by the
     * VoltServerConfig instance. Just pick from the list of listeners
     * randomly.
     *
     * @return A VoltClient instance connected to the server driven by the
     * VoltServerConfig instance.
     */
    public Client getClient() throws IOException {
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        final String listener = listeners.get(r.nextInt(listeners.size()));
        final Client client = ClientFactory.createClient();
        client.createConnection(null, listener, HStoreConstants.DEFAULT_PORT, m_username, m_password);
        m_clients.add(client);
        return client;
    }

    /**
     * Release a client instance and any resources associated with it
     */
    public void releaseClient(Client c) throws IOException, InterruptedException {
        boolean removed = m_clients.remove(c);
        assert(removed);
        c.close();
    }

    /**
     * Get a SocketChannel that is an authenticated connection to a server driven by the
     * VoltServerConfig instance. Just pick from the list of listeners
     * randomly.
     *
     * @return A SocketChannel that is already authenticated with the server
     */
    public SocketChannel getClientChannel() throws IOException {
        return getClientChannel(false);
    }
    public SocketChannel getClientChannel(final boolean noTearDown) throws IOException {
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        final String listener = listeners.get(r.nextInt(listeners.size()));
        final SocketChannel channel = (SocketChannel)
            ConnectionUtil.getAuthenticatedConnection(
                    listener,
                    m_username, m_password, HStoreConstants.DEFAULT_PORT)[0];
        channel.configureBlocking(true);
        if (!noTearDown) {
            synchronized (m_clientChannels) {
                m_clientChannels.add(channel);
            }
        }
        return channel;
    }

    /**
     * Protected method used by MultiConfigSuiteBuilder to set the VoltServerConfig
     * instance a particular test will run with.
     *
     * @param config An instance of VoltServerConfig to run tests with.
     */
    void setConfig(final VoltServerConfig config) {
        m_config = config;
    }


    @Override
    public String getName() {
        // munge the test name with the VoltServerConfig instance name
        return super.getName() + "-" + m_config.getName();
    }
}
