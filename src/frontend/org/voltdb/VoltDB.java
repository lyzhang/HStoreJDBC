/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.File;
import java.util.TimeZone;

import edu.brown.hstore.HStoreConstants;

/**
 * <code>VoltDB</code> is the main class for VoltDB server.
 * It sets up global objects and then starts the individual threads
 * for the <code>ThreadManager</code>s.
 */
@Deprecated
public class VoltDB {

    static final int INITIATOR_SITE_ID = 0;
    public static final int DTXN_MAILBOX_ID = 0;

    public static final int SITES_TO_HOST_DIVISOR = 100;
    public static final int MAX_SITES_PER_HOST = 128;

    // if VoltDB is running in your process, prepare to us GMT timezone
    public synchronized static void setDefaultTimezone() {
        //System.out.println(TimeZone.getTimeZone("GMT+0").getID());
          TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
    }
    static {
        setDefaultTimezone();
    }

    /** Encapsulates VoltDB configuration parameters */
    @Deprecated
    public static class Configuration {

        /** Whether to enable watchdogs to check for possible deadlocks **/
        public boolean m_useWatchdogs = false;

        /** use normal JNI backend or optional IPC or HSQLDB backends */
        public BackendTarget m_backend = BackendTarget.NATIVE_EE_JNI;
        /** name of the m_catalog JAR file */
        public String m_pathToCatalog = "catalog.jar";
        /** level of internal transaction profiling (for testing) */
        public ProcedureProfiler.Level m_profilingLevel =
            ProcedureProfiler.Level.DISABLED;
        /** false if voltdb.so shouldn't be loaded (for example if JVM is
         *  started by voltrun).
         */
        public boolean m_noLoadLibVOLTDB = false;
        /** if set to true each execution site will tie its thread to a core
         *  in a round robin fashion
         */
        public boolean m_useThreadAffinity = false;
        /** port number for the first client interface for each server */
        public int m_port = HStoreConstants.DEFAULT_PORT;

        public boolean listenForDumpRequests = false;

        /**
         * Whether or not adhoc queries should generate debugging output
         */
        public boolean m_quietAdhoc = false;

        public Configuration() { }

        public Configuration(String args[]) {
            String arg;

            // Arguments are accepted in any order.
            //
            // options:
            // [noloadlib] [hsqldb|jni|ipc] [polite|intrusive] [catalog path_to_catalog]

            for (int i=0; i < args.length; ++i) {
                arg = args[i];
                if (arg.equals("noloadlib")) {
                    m_noLoadLibVOLTDB = true;
                }
                else if (arg.equals("ipc")) {
                    m_backend = BackendTarget.NATIVE_EE_IPC;
                }
                else if (arg.equals("jni")) {
                    m_backend = BackendTarget.NATIVE_EE_JNI;
                }
                else if (arg.equals("hsqldb")) {
                    m_backend = BackendTarget.HSQLDB_BACKEND;
                }
                else if (arg.equals("valgrind")) {
                    m_backend = BackendTarget.NATIVE_EE_VALGRIND_IPC;
                }
                else if (arg.equals("polite")) {
                    m_profilingLevel = ProcedureProfiler.Level.POLITE;
                }
                else if (arg.equals("intrusive")) {
                    m_profilingLevel = ProcedureProfiler.Level.INTRUSIVE;
                }
                else if (arg.equals("listenfordumps")) {
                    listenForDumpRequests = true;
                }
                else if (arg.equals("quietadhoc"))
                {
                    m_quietAdhoc = true;
                }
                // handle from the command line as two strings <catalog> <filename>
                else if (arg.equals("port")) {
                    m_port = Integer.parseInt(args[++i]);
                }
                else if (arg.startsWith("port ")) {
                    m_port = Integer.parseInt(arg.substring("port ".length()));
                }
                else if (arg.equals("catalog")) {
                    m_pathToCatalog = args[++i];
                }
                // and from ant as a single string "m_catalog filename"
                else if (arg.startsWith("catalog ")) {
                    m_pathToCatalog = arg.substring("catalog ".length());
                }
                else if (arg.equalsIgnoreCase("useThreadAffinity")) {
                    m_useThreadAffinity = true;
                } else if (arg.equalsIgnoreCase("useWatchdogs")) {
                    m_useWatchdogs = true;
                } else {
                    /*
                     * N.B: this text is user visible. It intentionally does NOT reveal
                     * options not interesting to, say, the casual VoltDB operator.
                     * Please do not reveal options not documented in the VoltDB
                     * documentation set. (See GettingStarted.pdf).
                     */
                    System.out.println("Unrecognized option to VoltDB: " + arg);
                    System.out.println("Usage: org.voltdb.VoltDB catalog <catalog.jar>");
                    System.out.println("The _Getting Started With VoltDB_ book explains how to run " +
                                       " VoltDB from the command line.");
                    System.exit(-1);
                }
            }
        }
        /** Helper to set the path for compiled jar files.
         *  Could also live in VoltProjectBuilder but any code that creates
         *  a catalog will probably start VoltDB with a Configuration
         *  object. Perhaps this is more convenient?
         * @return the path chosen for the catalog.
         */
        public String setPathToCatalogForTest(String jarname) {
            m_pathToCatalog = getPathToCatalogForTest(jarname);
            return m_pathToCatalog;
        }
        public static String getPathToCatalogForTest(String jarname) {
            String answer = jarname;
            if (System.getenv("TEST_DIR") != null) {
                answer = System.getenv("TEST_DIR") + File.separator + jarname;
            }
            return answer;
        }
    }

    private static VoltDB.Configuration m_config = new VoltDB.Configuration();

    /* helper functions to access current configuration values */
    public static boolean getLoadLibVOLTDB() {
        return !(m_config.m_noLoadLibVOLTDB);
    }

    public static BackendTarget getEEBackendType() {
        return m_config.m_backend;
    }

    public static boolean getUseThreadAffinity() {
        return m_config.m_useThreadAffinity;
    }

    public static boolean getUseWatchdogs() {
        return m_config.m_useWatchdogs;
    }

    public static boolean getQuietAdhoc()
    {
        return m_config.m_quietAdhoc;
    }

    /**
     * Entry point for the VoltDB server process.
     *
     * @param args  Optional single string value which represents
     *              path to m_catalog jar.
     */
    public static void main(String args[]) {
        //Thread.setDefaultUncaughtExceptionHandler(new VoltUncaughtExceptionHandler());
        initialize(new Configuration(args));
        instance().run();
    }

    /**
     * Initialize the VoltDB server.
     *
     * @param config  The VoltDB.Configuration to use to initialize the server.
     */
    public static void initialize(VoltDB.Configuration config)
    {
        m_config = config;
        instance().initialize(config);
    }

    /**
     * Retrieve a reference to the object implementing VoltDBInterface.  When
     * running a real server (and not a test harness), this instance will only
     * be useful after calling VoltDB.initialize().
     *
     * @return A reference to the underlying VoltDBInterface object.
     */
    public static VoltDBInterface instance() {
        return null; //singleton;
    }

    /**
     * Useful only for unit testing.
     *
     * Replace the default VoltDB server instance with an instance of
     * VoltDBInterface that is used for testing.
     *
     */
    public static void replaceVoltDBInstanceForTest(VoltDBInterface testInstance)
    {
        // singleton = testInstance;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

}
