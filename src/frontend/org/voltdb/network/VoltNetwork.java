/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.network;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.EstTimeUpdater;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltLoggerFactory;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;

/** Produces work for registered ports that are selected for read, write */
 public class VoltNetwork implements Runnable {
    
    private static final Logger m_logger = Logger.getLogger(VoltNetwork.class);
    private static final Logger networkLog =
        Logger.getLogger("NETWORK", VoltLoggerFactory.instance());
    
    private final Selector m_selector;
    private final ArrayDeque<Runnable> m_tasks = new ArrayDeque<Runnable>();
    // keep two lists and swap them in and out to minimize contention
    private final ArrayDeque<VoltPort> m_selectorUpdates_1 = new ArrayDeque<VoltPort>();//Used as the lock for swapping lists
    private final ArrayDeque<VoltPort> m_selectorUpdates_2 = new ArrayDeque<VoltPort>();
    private ArrayDeque<VoltPort> m_activeUpdateList = m_selectorUpdates_1;
    private volatile boolean m_shouldStop = false;//volatile boolean is sufficient
    private final Thread m_thread;
    private final HashSet<VoltPort> m_ports = new HashSet<VoltPort>();
    private final boolean m_useBlockingSelect;
    private final boolean m_useExecutorService;
    private final ArrayList<WeakReference<Thread>> m_networkThreads = new ArrayList<WeakReference<Thread>>();
    private final ArrayList<DBBPool> m_poolsToClearOnShutdown = new ArrayList<DBBPool>();

    /**
     * Synchronizes registration and unregistration of channels
     */
    private final ReentrantReadWriteLock m_registrationLock = new ReentrantReadWriteLock();

    /**
     * Start this VoltNetwork's thread;
     */
    public void start() {
        m_thread.start();
    }

    /** Used for test only! */
    public VoltNetwork(Selector selector) {
        m_thread = null;
        m_selector = selector;
        m_useBlockingSelect = true;
        m_useExecutorService = false;
    }

    public VoltNetwork() {
        this(true, true, null, null);
    }
    
    public VoltNetwork(HStoreSite hstore_site) {
        this(true, true, null, hstore_site);
    }
    
    public VoltNetwork(boolean useExecutorService, boolean blockingSelect, Integer threads) {
        this(useExecutorService, blockingSelect, threads, null);
    }

    /**
     * Initialize a m_selector and become ready to perform real work
     * If the network is not going to provide any threads provideOwnThread should be false
     * and runOnce should be called periodically
     **/
    public VoltNetwork(boolean useExecutorService, boolean blockingSelect, Integer threads, final HStoreSite hstore_site) {
        m_thread = new Thread(this, "Volt Network");
        m_thread.setDaemon(true);
        m_useBlockingSelect = blockingSelect;

        try {
            m_selector = Selector.open();
        } catch (IOException ex) {
            m_logger.fatal(null, ex);
            throw new RuntimeException(ex);
        }

        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        //Single thread is plenty for 4 cores.
        if (availableProcessors <= 4) {
            m_useExecutorService = false;
        } else {
            m_useExecutorService = useExecutorService;
        }
        if (!m_useExecutorService) {
            return;
        }

        int threadPoolSize = 1;
        if (threads != null) {
            threadPoolSize = threads.intValue();
        } else if (availableProcessors <= 4) {
            threadPoolSize = 1;
        } else if (availableProcessors <= 8) {
            threadPoolSize = 2;
        } else if (availableProcessors <= 16) {
            threadPoolSize = 3;
        } else {
            threadPoolSize = 3;
        }
        m_logger.debug("Network Thread Pool Size: " + threadPoolSize);

        final ThreadFactory tf = new ThreadFactory() {
            private ThreadGroup group = new ThreadGroup(Thread.currentThread().getThreadGroup(), "Network Threads");
            private int threadIndex = 0;
            @Override
            public Thread newThread(final Runnable run) {
                String threadName = String.format("%s-%02d", HStoreConstants.THREAD_NAME_INCOMINGNETWORK, this.threadIndex++);
                if (hstore_site != null) {
                    threadName = HStoreThreadManager.getThreadName(hstore_site, threadName);
                }
                
                final Thread t = new Thread(this.group, run, threadName) {
                    @Override
                    public void run() {
                        // HACK: Register the thread if we have an HStoreThreadManager
                        if (hstore_site != null) {
                            hstore_site.getThreadManager().registerProcessingThread();
                        }
                        try {
                            run.run();
                        } finally {
                            synchronized (m_poolsToClearOnShutdown) {
                                m_poolsToClearOnShutdown.add(VoltPort.m_pool.get());
                            }
                        }
                    };
                };
                synchronized (m_networkThreads) {
                    m_networkThreads.add(new WeakReference<Thread>(t));
                }
                t.setDaemon(true);
                return t;
            }

        };

        for (int ii = 0; ii < threadPoolSize; ii++) {
            tf.newThread(new Runnable() {
                @Override
                public void run() {
                    //final ArrayDeque<VoltPortFutureTask> nextTasks = new ArrayDeque<VoltPortFutureTask>(3);0
                    while (true) {
                        try {
                            Runnable nextTask = null;
                            synchronized (m_tasks) {
                                nextTask = m_tasks.poll();
                                while (nextTask == null && !m_shouldStop) {
                                    m_tasks.wait();
                                    nextTask = m_tasks.poll();
                                }
                            }
                            if (nextTask == null) {
                                return;
                            }

                            nextTask.run();
                        } catch (InterruptedException e) {
                            return;
                        } catch (Exception e) {
                            networkLog.error(e);
                        }
                    }
                }
            }).start();
        }

        //It is really handy to be able to uncomment this and print bandwidth usage. Hopefully
        //management tools will replace it.
//        new Thread() {
//            @Override
//            public void run() {
//                long last = System.currentTimeMillis();
//                while (true) {
//                    try {
//                        Thread.sleep(10000);
//                    } catch (InterruptedException e) {
//                        // TODO Auto-generated catch block
//                        e.printStackTrace();
//                    }
//                    final long now = System.currentTimeMillis();
//                    long totalRead = 0;
//                    long totalMessagesRead = 0;
//                    long totalWritten = 0;
//                    long totalMessagesWritten = 0;
//                    synchronized (m_ports) {
//                        for (VoltPort p : m_ports) {
//                            final long read = p.readStream().getBytesRead(true);
//                            final long writeInfo[] = p.writeStream().getBytesAndMessagesWritten(true);
//                            final long messagesRead = p.getMessagesRead(true);
//                            totalRead += read;
//                            totalMessagesRead += messagesRead;
//                            totalWritten += writeInfo[0];
//                            totalMessagesWritten += writeInfo[1];
//                        }
//                    }
//                    double delta = (now - last) / 1000.0;
//                    double mbRead = totalRead / (1024.0 * 1024.0);
//                    double mbWritten = totalWritten / (1024.0 * 1024.0);
//                    System.out.printf("Transferred %.2f/%.2f (IN/OUT)/sec\n", mbRead / delta, mbWritten / delta);
//                    last = now;
//                }
//            }
//        }.start();
    }


    /**
     * Lock that causes the selection thread to wait for all threads that
     * are in the process of registering or unregistering channels to finish
     */
    private void waitForRegistrationLock() {
        m_registrationLock.writeLock().lock();
        m_registrationLock.writeLock().unlock();
    }

    /**
     * Acquire a lock that stops the selection thread while a channel is being registered/unregistered
     */
    private void acquireRegistrationLock() {
        m_registrationLock.readLock().lock();
        m_selector.wakeup();
    }

    /**
     * Release a lock that stops the selection thread while a channel is being registered/unregistered
     */
    private void releaseRegistrationLock() {
        m_registrationLock.readLock().unlock();
    }

    /** Instruct the network to stop after the current loop */
    public void shutdown() throws InterruptedException {
        if (m_thread != null) {
            synchronized (this) {
                m_shouldStop = true;
                m_selector.wakeup();
            }
            m_thread.join();
        } else {
            m_shouldStop = true;
        }
    }

    public Connection registerChannel(SocketChannel channel, InputHandler handler) throws IOException {
        return registerChannel(channel, handler, SelectionKey.OP_READ);
    }

    /**
     * Register a channel with the selector and create a Connection that will pass incoming events
     * to the provided handler.
     * @param channel
     * @param handler
     * @throws IOException
     */
    public Connection registerChannel(
            SocketChannel channel,
            InputHandler handler,
            int interestOps) throws IOException {
        channel.configureBlocking (false);
        channel.socket().setKeepAlive(true);

        VoltPort port =
            new VoltPort(
                    this,
                    handler,
                    handler.getExpectedOutgoingMessageSize(),
                    channel.socket().getInetAddress().getHostName());
        port.registering();

        acquireRegistrationLock();
        try {
            SelectionKey key = channel.register (m_selector, interestOps, port);

            port.setKey (key);
            port.registered();

            return port;
        } finally {
            synchronized (m_ports) {
                m_ports.add(port);
            }
            releaseRegistrationLock();
        }
    }

    /**
     * Unregister a channel. The connections streams are not drained before finishing.
     * @param c
     */
    void unregisterChannel (Connection c) {
        VoltPort port = (VoltPort)c;
        assert(c != null);
        SelectionKey selectionKey = port.getKey();

        acquireRegistrationLock();
        try {
            synchronized (m_ports) {
                if (!m_ports.contains(port)) {
                    return;
                }
            }
            port.unregistering();
            selectionKey.cancel();
            selectionKey.attach(null);
            synchronized (m_ports) {
                m_ports.remove(port);
            }
        } finally {
            releaseRegistrationLock();
        }
        port.unregistered();
    }

    /** Set interest registrations for a port */
    public void addToChangeList(VoltPort port) {
        synchronized (m_selectorUpdates_1) {
            m_activeUpdateList.add(port);
        }
        if (m_useBlockingSelect) {
            m_selector.wakeup();
        }
    }

    @Override
    public void run() {
        try {
            while (m_shouldStop == false) {
                try {
                    while (m_shouldStop == false) {
                        waitForRegistrationLock();
                        if (m_useBlockingSelect) {
                            m_selector.select(5);
                        } else {
                            m_selector.selectNow();
                        }
                        installInterests();
                        invokeCallbacks();
                        EstTimeUpdater.update(System.currentTimeMillis());
                    }
                } catch (Exception ex) {
                    m_logger.error(null, ex);
                }
            }
        } finally {
            p_shutdown();
        }
    }

    private synchronized void p_shutdown() {
        //Synchronized so the interruption won't interrupt the network thread
        //while it is waiting for the executor service to shutdown
        try {
            try {
                synchronized (m_networkThreads) {
                    synchronized (m_tasks) {
                        m_tasks.notifyAll();
                    }
                    for (final WeakReference<Thread> r : m_networkThreads) {
                        final Thread t = r.get();
                        if (t != null) {
                            t.join();
                        }
                    }
                }
            } catch (InterruptedException e) {
                m_logger.error(e);
            }

            Set<SelectionKey> keys = m_selector.keys();

            for (SelectionKey key : keys) {
                VoltPort port = (VoltPort) key.attachment();
                if (port != null) {
                    try {
                        unregisterChannel (port);
                    } catch (Exception e) {
                        networkLog.error("Exception unregisering port " + port, e);
                    }
                }
            }

            synchronized (m_poolsToClearOnShutdown) {
                for (DBBPool p : m_poolsToClearOnShutdown) {
                    p.clear();
                }
                m_poolsToClearOnShutdown.clear();
            }

            try {
                m_selector.close();
            } catch (IOException e) {
                m_logger.error(null, e);
            }
        } finally {
            this.notifyAll();
        }
    }

    protected void installInterests() {
        // swap the update lists to avoid contention while
        // draining the requested values. also guarantees
        // that the end of the list will be reached if code
        // appends to the update list without bound.
        ArrayDeque<VoltPort> oldlist;
        synchronized(m_selectorUpdates_1) {
            if (m_activeUpdateList == m_selectorUpdates_1) {
                oldlist = m_selectorUpdates_1;
                m_activeUpdateList = m_selectorUpdates_2;
            }
            else {
                oldlist = m_selectorUpdates_2;
                m_activeUpdateList = m_selectorUpdates_1;
            }
        }

        while (!oldlist.isEmpty()) {
            final VoltPort port = oldlist.poll();
            try {
                if (port.isRunning()) {
                    continue;
                }
                if (port.isDead()) {
                    unregisterChannel(port);
                    try {
                        port.m_selectionKey.channel().close();
                    } catch (IOException e) {}
                } else if (port.hasQueuedRunnables()) {
                        port.lockForHandlingWork();
                        port.getKey().interestOps(0);
                    m_selector.selectedKeys().remove(port.getKey());
                    synchronized (m_tasks) {
                        m_tasks.offer(getPortCallRunnable(port));
                        m_tasks.notify();
                    }
                } else {
                    resumeSelection(port);
                }
            } catch (java.nio.channels.CancelledKeyException e) {
                networkLog.warn(
                        "Had a cancelled key exception while processing queued runnables for port "
                        + port.m_remoteHost, e);
            }
        }
    }

    private void resumeSelection( VoltPort port) {
        SelectionKey key = port.getKey();

        if (key.isValid()) {
            key.interestOps (port.interestOps());
        } else {
            synchronized (m_ports) {
                m_ports.remove(port);
            }
        }
    }

    private Runnable getPortCallRunnable(final VoltPort port) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    port.call();
                } catch (Exception e) {
                    port.die();
                    if (e instanceof IOException) {
                        m_logger.trace( "VoltPort died, probably of natural causes", e);
                    } else {
                        networkLog.error( "VoltPort died due to an unexpected exception", e);
                    }
                } finally {
                    addToChangeList (port);
                }
            }
        };
    }

    /** Set the selected interest set on the port and run it. */
    protected void invokeCallbacks() {
        final Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
        ArrayList<Runnable> generatedTasks = null;
        for(SelectionKey key : selectedKeys) {
            final VoltPort port = (VoltPort) key.attachment();
            if (port == null) {
                continue;
            }
            try {
                port.lockForHandlingWork();
                key.interestOps(0);

                final Runnable runner = getPortCallRunnable(port);

                if (m_useExecutorService) {
                    if (generatedTasks == null) generatedTasks = new ArrayList<Runnable>();
                    generatedTasks.add(runner);
                } else {
                    runner.run();
                }
            }
            catch (CancelledKeyException e) {
                e.printStackTrace();
                // no need to do anything here until
                // shutdown makes more sense
            }
        }

        if (generatedTasks != null && !generatedTasks.isEmpty()) {
            synchronized (m_tasks) {
                m_tasks.addAll(generatedTasks);
                if (m_tasks.size() > 1) {
                    m_tasks.notifyAll();
                } else {
                    m_tasks.notify();
                }
            }
        }

        selectedKeys.clear();
    }

    public Map<Long, Pair<String, long[]>> getIOStats(boolean interval) {
        final HashMap<Long, Pair<String, long[]>> retval =
            new HashMap<Long, Pair<String, long[]>>();
        long totalRead = 0;
        long totalMessagesRead = 0;
        long totalWritten = 0;
        long totalMessagesWritten = 0;
        synchronized (m_ports) {
            for (VoltPort p : m_ports) {
                final long read = p.readStream().getBytesRead(interval);
                final long writeInfo[] = p.writeStream().getBytesAndMessagesWritten(interval);
                final long messagesRead = p.getMessagesRead(interval);
                totalRead += read;
                totalMessagesRead += messagesRead;
                totalWritten += writeInfo[0];
                totalMessagesWritten += writeInfo[1];
                retval.put(
                        p.connectionId(),
                        Pair.of(
                                p.m_remoteHost,
                                new long[] {
                                        read,
                                        messagesRead,
                                        writeInfo[0],
                                        writeInfo[1] }));
            }
        }
        retval.put(
                -1L,
                Pair.of(
                        "GLOBAL",
                        new long[] {
                                totalRead,
                                totalMessagesRead,
                                totalWritten,
                                totalMessagesWritten }));
        return retval;
    }

    public ArrayList<Long> getThreadIds() {
        ArrayList<Long> ids = new ArrayList<Long>();
        if (m_thread != null) {
            ids.add(m_thread.getId());
        }
        for (WeakReference<Thread> ref : m_networkThreads) {
            ids.add(ref.get().getId());
        }
        return ids;
    }
}
