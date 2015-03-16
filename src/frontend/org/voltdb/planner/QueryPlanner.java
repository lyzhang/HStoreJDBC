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

package org.voltdb.planner;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.planner.microoptimizations.MicroOptimizationRunner;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.BuildDirectoryUtils;

import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * The query planner accepts catalog data, SQL statements from the catalog, then
 * outputs the plan with the lowest cost according to the cost model.
 *
 */
public class QueryPlanner {
    private static final Logger LOG = Logger.getLogger(QueryPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    
    private static boolean TPCE_WARNING = false;
    
    PlanAssembler m_assembler;
    HSQLInterface m_HSQL;
    DatabaseEstimates m_estimates;
    Cluster m_cluster;
    Database m_db;
    String m_recentErrorMsg;
    Throwable m_recentError;
    boolean m_useGlobalIds;
    boolean m_quietPlanner;
    final PlannerContext m_context;

    /**
     * Initialize planner with physical schema info and a reference to HSQLDB parser.
     *
     * @param catalogCluster Catalog info about the physical layout of the cluster.
     * @param catalogDb Catalog info about schema, metadata and procedures.
     * @param HSQL HSQLInterface pointer used for parsing SQL into XML.
     * @param useGlobalIds
     */
    public QueryPlanner(Cluster catalogCluster, Database catalogDb,
                        HSQLInterface HSQL, DatabaseEstimates estimates,
                        boolean useGlobalIds, boolean suppressDebugOutput) {
        assert(HSQL != null);
        assert(catalogCluster != null);
        assert(catalogDb != null);

        m_HSQL = HSQL;
        // PAVLO: We have to use the global singleton in order to get the same guids across queries
        m_context = PlannerContext.singleton(); // new PlannerContext();
        m_assembler = new PlanAssembler(m_context, catalogCluster, catalogDb);
        m_db = catalogDb;
        m_cluster = catalogCluster;
        m_estimates = estimates;
        m_useGlobalIds = useGlobalIds;
        m_quietPlanner = suppressDebugOutput;
    }

    /**
     * Get the best plan for the SQL statement given, assuming the given costModel.
     *
     * @param costModel The current cost model to evaluate plans with.
     * @param sql SQL stmt text to be planned.
     * @param stmtName The name of the sql statement to be planned.
     * @param procName The name of the procedure containing the sql statement to be planned.
     * @param singlePartition Is the stmt single-partition?
     * @param paramHints
     * @return The best plan found for the SQL statement or null if none can be found.
     */
    public CompiledPlan compilePlan(AbstractCostModel costModel, String sql, String stmtName, String procName, boolean singlePartition, ScalarValueHints[] paramHints) {
        assert(costModel != null);
        assert(sql != null);
        assert(stmtName != null);
        assert(procName != null);

        // reset any error message
        m_recentErrorMsg = null;

        // set the usage of global ids in the plan assembler
        PlanAssembler.setUseGlobalIds(m_useGlobalIds);

        // use HSQLDB to get XML that describes the semantics of the statement
        // this is much easier to parse than SQL and is checked against the catalog
        String xmlSQL = null;
        try {
            xmlSQL = m_HSQL.getXMLCompiledStatement(sql);
        } catch (HSQLParseException e) {
            if (debug.val) LOG.warn(String.format("Failed to retrieve compiled XML for %s.%s\n%s", procName, stmtName, sql));
            m_recentErrorMsg = e.getMessage();
            return null;
        }

        if (!m_quietPlanner)
        {
            // output the xml from hsql to disk for debugging
            PrintStream xmlDebugOut =
                BuildDirectoryUtils.getDebugOutputPrintStream("statement-hsql-xml", procName + "_" + stmtName + ".xml");
            xmlDebugOut.println(xmlSQL);
            xmlDebugOut.close();
        }

        // get a parsed statement from the xml
        AbstractParsedStmt initialParsedStmt = null;
        try {
            initialParsedStmt = AbstractParsedStmt.parse(sql, xmlSQL, m_db);
        }
        catch (Throwable e) {
            LOG.error(String.format("Failed to parse SQL for %s.%s", procName, stmtName), e);
            m_recentErrorMsg = e.getMessage();
            return null;
        }
        if (initialParsedStmt == null)
        {
            m_recentErrorMsg = "Failed to parse SQL statement: " + sql;
            return null;
        }

        if (!m_quietPlanner)
        {
            // output a description of the parsed stmt
            PrintStream parsedDebugOut =
                BuildDirectoryUtils.getDebugOutputPrintStream("statement-parsed", procName + "_" + stmtName + ".txt");
            parsedDebugOut.println(initialParsedStmt.toString());
            parsedDebugOut.close();
        }

        // get ready to find the plan with minimal cost
        CompiledPlan rawplan = null;
        CompiledPlan bestPlan = null;
        double minCost = Double.MAX_VALUE;

        HashMap<String, String> planOutputs = new HashMap<String, String>();
        HashMap<String, String> dotPlanOutputs = new HashMap<String, String>();
        String winnerName = "";

        // index of the currently being "costed" plan
        int i = 0;

        PlanStatistics stats = null;

        Integer tpce_limit = null;
        
        // iterate though all the variations on the abstract parsed stmts
        for (AbstractParsedStmt parsedStmt : ExpressionEquivalenceProcessor.getEquivalentStmts(initialParsedStmt)) {
            // ---------------------------------------------------------------
            //              uuuuuuuuuuuuuuuuuuuu
            //            u" uuuuuuuuuuuuuuuuuu "u
            //          u" u$$$$$$$$$$$$$$$$$$$$u "u
            //        u" u$$$$$$$$$$$$$$$$$$$$$$$$u "u
            //      u" u$$$$$$$$$$$$$$$$$$$$$$$$$$$$u "u
            //    u" u$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$u "u
            //  u" u$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$u "u
            //  $ $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ $
            //  $ $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ $
            //  $ $$$" ... "$...  ...$" ... "$$$  ... "$$$ $
            //  $ $$$u `"$$$$$$$  $$$  $$$$$  $$  $$$  $$$ $
            //  $ $$$$$$uu "$$$$  $$$  $$$$$  $$  """ u$$$ $
            //  $ $$$""$$$  $$$$  $$$u "$$$" u$$  $$$$$$$$ $
            //  $ $$$$....,$$$$$..$$$$$....,$$$$..$$$$$$$$ $
            //  $ $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ $
            //  "u "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" u"
            //    "u "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" u"
            //      "u "$$$$$$$$$$$$$$$$$$$$$$$$$$$$" u"
            //        "u "$$$$$$$$$$$$$$$$$$$$$$$$" u"
            //          "u "$$$$$$$$$$$$$$$$$$$$" u"
            //            "u """""""""""""""""" u"
            //              """"""""""""""""""""
            // ---------------------------------------------------------------
            // PAVLO TERRIBLE TERRIBLE TERRIBLE HACK!!!!
            // If this is the BrokerVolume query in TPC-E, then don't let it go past 25,000 iterations
            // ---------------------------------------------------------------
            if (parsedStmt.sql.contains("TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY")) {
                tpce_limit = 25000;
                LOG.warn("PAVLO: Enabled TPC-E BrokerVolume limit: " + tpce_limit);
            }

            // set up the plan assembler for this particular plan
            m_assembler.setupForNewPlans(parsedStmt, singlePartition);

            // loop over all possible plans
            while (true) {

                try {
                    rawplan = m_assembler.getNextPlan();
                }
                // on exception, set the error message and bail...
                catch (Throwable e) {
                    m_recentError = e;
                    m_recentErrorMsg = e.getMessage();
                    return null;
                }

                // stop this while loop when no more plans are generated
                if (rawplan == null)
                    break;

                // run the set of microptimizations, which may return many plans (or not)
                List<CompiledPlan> optimizedPlans = MicroOptimizationRunner.applyAll(rawplan);

                // iterate through the subset of plans
                for (CompiledPlan plan : optimizedPlans) {
                    // HACK: There is one query in TPC-E that our planner always chokes on and gets stuck
                    // in an infinite loop. So we'll print an error message and break out.
                    if (tpce_limit != null && tpce_limit-- <= 0) {
                        if (TPCE_WARNING == false) {
                            TPCE_WARNING = true;
                            LOG.warn("PAVLO: The TPC-E BrokerVolume BREAKOUT! The legend lives on!!!");
                        }
                        break;
                    }

                    // compute resource usage using the single stats collector
                    stats = new PlanStatistics();
                    AbstractPlanNode planGraph = plan.fragments.get(0).planGraph;
                    
                    // compute statistics about a plan
                    boolean result = planGraph.computeEstimatesRecursively(stats, m_cluster, m_db, m_estimates, paramHints);
                    assert(result);

                    // GENERATE JSON DEBUGGING OUTPUT BEFORE WE CLEAN UP THE PlanColumns
                    // convert a tree into an execution list
                    PlanNodeList nodeList = new PlanNodeList(planGraph);

                    // get the json serialized version of the plan
                    String json = "";
//                    try {
//                        String crunchJson = nodeList.toJSONString();
                        //System.out.println(crunchJson);
                        //System.out.flush();
                        /* FIXME
                        JSONObject jobj = new JSONObject(crunchJson);
                        json = jobj.toString(4);
                    } catch (JSONException e2) {
                        // Any plan that can't be serialized to JSON to
                        // write to debugging output is also going to fail
                        // to get written to the catalog, to sysprocs, etc.
                        // Just bail.
                        m_recentErrorMsg = "Plan for sql: '" + sql +
                                           "' can't be serialized to JSON";
                        return null;
                    }
                    */

                    // compute the cost based on the resources using the current cost model
                    double cost = costModel.getPlanCost(stats);

                    // find the minimum cost plan
                    if (cost < minCost) {
                        minCost = cost;
                        // free the PlanColumns held by the previous best plan
                        if (bestPlan != null) {
                            bestPlan.freePlan(m_context, plan.getColumnGuids());
                        }
                        bestPlan = plan;
                    } else {
                        plan.freePlan(m_context, bestPlan.getColumnGuids());
                    }

                    // output a description of the parsed stmt
                    String filename = String.valueOf(i++);
                    if (bestPlan == plan) winnerName = filename;
                    json = "COST: " + String.valueOf(cost) + "\n" + json;
                    planOutputs.put(filename, json);

                    // create a graph friendly version
                    dotPlanOutputs.put(filename, nodeList.toDOTString("name"));
                }
            }
            tpce_limit = null;
        }

        // make sure we got a winner
        if (bestPlan == null) {
            m_recentErrorMsg = "Unable to plan for statement. Error unknown.";
            return null;
        }

        // Validate that everything is there
        Set<Integer> bestPlan_columns = bestPlan.getColumnGuids(); 
        for (Integer column_guid : bestPlan_columns) {
            if (m_context.hasColumn(column_guid) == false) {
                m_recentErrorMsg = "Missing column guid " + column_guid;
                return (null);
            }
        } // FOR
        if (debug.val) LOG.debug(String.format("All columns are there for %s.%s: %s", procName, stmtName, bestPlan_columns));

        // reset all the plan node ids for a given plan
        bestPlan.resetPlanNodeIds();

        if (!m_quietPlanner)
        {
            // print all the plans to disk for debugging
            for (Entry<String, String> output : planOutputs.entrySet()) {
                String filename = output.getKey();
                if (winnerName.equals(filename)) {
                    filename = "WINNER " + filename;
                }
                PrintStream candidatePlanOut =
                    BuildDirectoryUtils.getDebugOutputPrintStream("statement-all-plans/" + procName + "_" + stmtName,
                                                                  filename + ".txt");

                candidatePlanOut.println(output.getValue());
                candidatePlanOut.close();
            }

            for (Entry<String, String> output : dotPlanOutputs.entrySet()) {
                String filename = output.getKey();
                if (winnerName.equals(filename)) {
                    filename = "WINNER " + filename;
                }
                PrintStream candidatePlanOut =
                    BuildDirectoryUtils.getDebugOutputPrintStream("statement-all-plans/" + procName + "_" + stmtName,
                                                                  filename + ".dot");

                candidatePlanOut.println(output.getValue());
                candidatePlanOut.close();
            }

            // output the plan statistics to disk for debugging
            PrintStream plansOut =
                BuildDirectoryUtils.getDebugOutputPrintStream("statement-stats", procName + "_" + stmtName + ".txt");
            plansOut.println(stats.toString());
            plansOut.close();
        }

        // PAVLO: Get the full plan json
        AbstractPlanNode root = bestPlan.fragments.get(0).planGraph;
//        String orig_debug = PlanNodeUtil.debug(root);
        assert(root != null);
        String json = null;
        try {
            JSONObject jobj = new JSONObject(new PlanNodeList(root).toJSONString());
            json = jobj.toString();
        } catch (JSONException e2) {
            throw new RuntimeException(String.format("Failed to serialize JSON query plan for %s.%s", procName, stmtName), e2);
        }
        assert(json != null);
        
        // split up the plan everywhere we see send/recieve into multiple plan fragments
        bestPlan = Fragmentizer.fragmentize(bestPlan, m_db);
        bestPlan.fullplan_json = json;
        if (debug.val) LOG.debug(String.format("Stored serialized JSON query plan for %s.%s", procName, stmtName));
        
        // PAVLO:
//        if (singlePartition == false && procName.equalsIgnoreCase("GetTableCounts") && stmtName.equalsIgnoreCase("HistoryCount")) {
//            System.err.println(sql + "\n+++++++++++++++++++++++++++++++++");
//            
//            System.err.println("ORIGINAL:\n" + orig_debug + StringUtil.SINGLE_LINE);
//            System.err.println("NEW:");
//            
//            for (int ii = 0; ii < bestPlan.fragments.size(); ii++) {
//                Fragment f = bestPlan.fragments.get(ii);
//                System.err.println(String.format("Fragment #%02d\n%s\n", ii, PlanNodeUtil.debug(f.planGraph)));
//            }
//            System.err.println(StringUtil.DOUBLE_LINE);
//            System.exit(1);
//        }

        // DTXN/EE can't handle plans that have more than 2 fragments yet.
//        if (bestPlan.fragments.size() > 2) {
//            m_recentErrorMsg = "Unable to plan for statement. Likely statement is "+
//                "joining two partitioned tables in a multi-partition stamtent. " +
//                "This is not supported at this time.";
//            return null;
//        }

        return bestPlan;
    }

    public PlannerContext getPlannerContext() {
        return m_context;
    }
    public Throwable getError() {
        return m_recentError;
    }
    public String getErrorMessage() {
        return m_recentErrorMsg;
    }
}
