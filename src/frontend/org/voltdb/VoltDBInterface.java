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

import java.util.List;
import java.util.Map;

import org.voltdb.network.VoltNetwork;

import edu.brown.hstore.PartitionExecutor;

@Deprecated
public interface VoltDBInterface
{
    /**
     * Whether calls to crashVoltDB should result in a crash or be ignored
     */
    public boolean ignoreCrash();

    public void readBuildInfo();

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    public void initialize(VoltDB.Configuration config);

    /**
     * Start all the site's event loops. That's it.
     */
    public void run();

    /**
     * Try to shut everything down so they system is ready to call
     * initialize again.
     * @param mainSiteThread The thread that m_inititalized the VoltDB or
     * null if called from that thread.
     */
    public void shutdown(Thread mainSiteThread) throws InterruptedException;

    public void startSampler();

    public VoltDB.Configuration getConfig();
    public CatalogContext getCatalogContext();
    public String getBuildString();
    public String getVersionString();
    public Object[] getInstanceId();
    public Map<Integer, PartitionExecutor> getLocalSites();
    public VoltNetwork getNetwork();
    public StatsAgent getStatsAgent();
    public BackendTarget getBackendTargetType();

    /**
     * Update the global logging context in the server.
     *
     * @param xmlConfig The xml string containing the new logging configuration
     * @param currentTxnId  The transaction ID at which this method is called
     */
    void logUpdate(String xmlConfig, long currentTxnId);

    /**
     * Updates the catalog context stored by this VoltDB without destroying the old one,
     * in case anything still links to it.
     *
     * @param newCatalogURL A URL for the new catalog (http: or file:).
     * @param diffCommands The commands to update the current catalog to the new one.
     * @param expectedCatalogVersion The version of the catalog the commands are targeted for.
     * @param currentTxnId  The transaction ID at which this method is called
     */
   public void catalogUpdate(String diffCommands, String newCatalogURL,
           int expectedCatalogVersion, long currentTxnId);

   /**
    * Updates the physical cluster configuration stored in the catalog at this server.
    *
    * @param diffCommands  The catalog commands that will update the cluster config
    */
   void clusterUpdate(String diffCommands);

   /**
     * Tells if the VoltDB is running. m_isRunning needs to be set to true
     * when the run() method is called, and set to false when shutting down.
     *
     * @return true if the VoltDB is running.
     */
    public boolean isRunning();
}
