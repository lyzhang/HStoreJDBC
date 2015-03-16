/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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

package org.voltdb.benchmark.tpcc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.benchmark.Clock;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.types.TimestampType;

import edu.brown.hashing.DefaultHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class TPCCSimulation {
    private static final Logger LOG = Logger.getLogger(TPCCSimulation.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public interface ProcCaller {
        public void callResetWarehouse(long w_id, long districtsPerWarehouse,
                long customersPerDistrict, long newOrdersPerDistrict)
        throws IOException;
        public void callStockLevel(short w_id, byte d_id, int threshold) throws IOException;
        public void callOrderStatus(String proc, Object... paramlist) throws IOException;
        public void callDelivery(short w_id, int carrier, TimestampType date) throws IOException;
        public void callPaymentByName(short w_id, byte d_id, double h_amount,
                short c_w_id, byte c_d_id, String c_last, TimestampType now) throws IOException;
        public void callPaymentById(short w_id, byte d_id, double h_amount,
                short c_w_id, byte c_d_id, int c_id, TimestampType now)
        throws IOException;
        public void callNewOrder(boolean rollback, boolean noop, Object... paramlist) throws IOException;
    }


    private final TPCCSimulation.ProcCaller client;
    private final RandomGenerator generator;
    private final Clock clock;
    public ScaleParameters parameters;
    private final long affineWarehouse;
    private final double skewFactor;
    private final TPCCConfig config;
    
    /**
     * W_ID -> List of W_IDs on Remote Sites
     */
    public static HashMap <Integer, List<Integer>> remoteWarehouseIds = null;
    
    protected static long lastAssignedWarehouseId = 1;
    
	private RandomDistribution.HotWarmCold custom_skew; 
    private RandomDistribution.Zipf zipf;
    
    private int tick_counter = 0;
    private int temporal_counter = 0;
    private final FastIntHistogram lastWarehouseHistory = new FastIntHistogram(true);
    private final FastIntHistogram totalWarehouseHistory = new FastIntHistogram(true);

    public TPCCSimulation(TPCCSimulation.ProcCaller client, RandomGenerator generator,
                          Clock clock, ScaleParameters parameters, TPCCConfig config, double skewFactor,
                          CatalogContext catalogContext) {
        assert parameters != null;
        this.client = client;
        this.generator = generator;
        this.clock = clock;
        this.parameters = parameters;
        this.affineWarehouse = lastAssignedWarehouseId;
        this.skewFactor = skewFactor;
        this.config = config;

        if (config.neworder_skew_warehouse) {
            if (debug.val) LOG.debug("Enabling W_ID Zipfian Skew: " + skewFactor);
            this.zipf = new RandomDistribution.Zipf(new Random(),
                                                    parameters.starting_warehouse,
                                                    parameters.last_warehouse+1,
                                                    Math.max(1.001d, this.skewFactor));

			this.custom_skew = new RandomDistribution.HotWarmCold(new Random(), 
																  parameters.starting_warehouse+1,
																  parameters.last_warehouse,
																  TPCCConstants.HOT_DATA_WORKLOAD_SKEW, TPCCConstants.HOT_DATA_SIZE, 
																  TPCCConstants.WARM_DATA_WORKLOAD_SKEW, TPCCConstants.WARM_DATA_SIZE);
        }
        if (config.warehouse_debug) {
            LOG.info("Enabling WAREHOUSE debug mode");
        }

        lastAssignedWarehouseId += 1;
        if (lastAssignedWarehouseId > parameters.last_warehouse)
            lastAssignedWarehouseId = 1;
        
        if (debug.val) {
            LOG.debug(this.toString());
        }
        if (config.neworder_multip_remote) {
            synchronized (TPCCSimulation.class) {
                if (remoteWarehouseIds == null) {
                	remoteWarehouseIds = new HashMap<Integer, List<Integer>>();
                	HashMap <Integer, Integer> partitionToSite = new HashMap<Integer, Integer>();
                	
                	DefaultHasher hasher = new DefaultHasher(catalogContext);
            		for (Site s: catalogContext.sites) {
            			for (Partition p: s.getPartitions())
            				partitionToSite.put(p.getId(), s.getId());
            		} // FOR
                		
            		for (int w_id0 = parameters.starting_warehouse; w_id0 <= parameters.last_warehouse; w_id0++) {
            		    final int partition0 = hasher.hash(w_id0);
            			final int site0 = partitionToSite.get(partition0);
            			final List<Integer> rList = new ArrayList<Integer>();	
            			
            			for (int w_id1 = parameters.starting_warehouse; w_id1 <= parameters.last_warehouse; w_id1++) {
            			    // Figure out what partition this W_ID maps to
            			    int partition1 = hasher.hash(w_id1);
            			    
            			    // Check whether this partition is on our same local site
            			    int site1 = partitionToSite.get(partition1);
            			    if (site0 != site1) rList.add(w_id1);
            			} // FOR
            			remoteWarehouseIds.put(w_id0, rList);
                	} // FOR
            		
            		LOG.debug("NewOrder Remote W_ID Mapping\n" + StringUtil.formatMaps(remoteWarehouseIds));
                }
            } // SYNCH
        }
    }
    
    protected Random rng() {
        return generator.rng();
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Warehouses", parameters.warehouses);
        m.put("W_ID Range", String.format("[%d, %d]", parameters.starting_warehouse, parameters.last_warehouse));
        m.put("Districts per Warehouse", parameters.districtsPerWarehouse);
        m.put("Custers per District", parameters.customersPerDistrict);
        m.put("Initial Orders per District", parameters.newOrdersPerDistrict);
        m.put("Items", parameters.num_items);
        m.put("Affine Warehouse", lastAssignedWarehouseId);
        m.put("Skew Factor", this.skewFactor);
        if (this.zipf != null && this.zipf.isHistoryEnabled()) {
            m.put("Skewed Warehouses", this.zipf.getHistory());
        }
        return ("TPCC Simulator Options\n" + StringUtil.formatMaps(m, this.config.debugMap()));
    }
    
    protected RandomDistribution.Zipf getWarehouseZipf() {
        return (this.zipf);
    }
    
    public synchronized void tick(int counter) {
        this.tick_counter = counter;
        if (config.warehouse_debug) {
            Map<String, Histogram<Integer>> m = new ListOrderedMap<String, Histogram<Integer>>();
            m.put(String.format("LAST ROUND\n - SampleCount=%d", this.lastWarehouseHistory.getSampleCount()),
                  this.lastWarehouseHistory);
            m.put(String.format("TOTAL\n - SampleCount=%d", this.totalWarehouseHistory.getSampleCount()),
                  this.totalWarehouseHistory);
            
            long total = this.totalWarehouseHistory.getSampleCount();
            LOG.info(String.format("ROUND #%02d - Warehouse Temporal Skew - %d / %d [%.2f]\n%s",
                    this.tick_counter, this.temporal_counter, total, (this.temporal_counter / (double)total), 
                    StringUtil.formatMaps(m)));
            LOG.info(StringUtil.SINGLE_LINE);
            this.lastWarehouseHistory.clearValues();
        }
    }

    private short generateWarehouseId() {
        short w_id = -1;
        
        // WAREHOUSE AFFINITY
        if (config.warehouse_affinity) {
            w_id = (short)this.affineWarehouse;
        } 
        // TEMPORAL SKEW
        else if (config.temporal_skew) {
            if (generator.number(1, 100) <= config.temporal_skew_mix) {
                if (config.temporal_skew_rotate) {
                    w_id = (short)((this.tick_counter % parameters.warehouses) + parameters.starting_warehouse);
                } else {
                    w_id = (short)config.first_warehouse;
                }
                this.temporal_counter++;
            } else {
                w_id = (short)generator.number(parameters.starting_warehouse, parameters.last_warehouse);
            }
        }
        // ZIPFIAN SKEWED WAREHOUSE ID
        else if (config.neworder_skew_warehouse) {
            assert(this.zipf != null);
            //w_id = (short)this.zipf.nextInt();
            if (generator.number(1, 100) <= config.temporal_skew_mix) {
                w_id = (short)this.custom_skew.nextInt();
            } else {
                w_id = (short)generator.number(parameters.starting_warehouse, parameters.last_warehouse);
            }
        }
        // GAUSSIAN SKEWED WAREHOUSE ID
        else if (skewFactor > 0.0d) {
            w_id = (short)generator.skewedNumber(parameters.starting_warehouse, parameters.last_warehouse, skewFactor);
        }
        // UNIFORM DISTRIBUTION
        else {
            w_id = (short)generator.number(parameters.starting_warehouse, parameters.last_warehouse);
        }
        
        assert(w_id >= parameters.starting_warehouse) : String.format("Invalid W_ID: %d [min=%d, max=%d]", w_id, parameters.starting_warehouse, parameters.last_warehouse); 
        assert(w_id <= parameters.last_warehouse) : String.format("Invalid W_ID: %d [min=%d, max=%d]", w_id, parameters.starting_warehouse, parameters.last_warehouse);
        
        this.lastWarehouseHistory.put(w_id);
        this.totalWarehouseHistory.put(w_id);
            
        return w_id;
    }
    
    // ----------------------------------------------------------------------------
    // REMOTE WAREHOUSE SELECTION METHODS
    // ----------------------------------------------------------------------------

    public static short generatePairedWarehouse(int w_id, int starting_warehouse, int last_warehouse) {
        int remote_w_id = (w_id % 2 == 0 ? w_id-1 : w_id+1);
        if (remote_w_id < starting_warehouse) remote_w_id = last_warehouse;
        else if (remote_w_id > last_warehouse) remote_w_id = starting_warehouse;
        return (short)remote_w_id;
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    private byte generateDistrict() {
        return (byte)generator.number(1, parameters.districtsPerWarehouse);
    }

    private int generateCID() {
        return generator.NURand(1023, 1, parameters.customersPerDistrict);
    }

    private int generateItemID() {
        return generator.NURand(8191, 1, parameters.num_items);
    }

    /** Executes a reset warehouse transaction. */
    public void doResetWarehouse() throws IOException {
        long w_id = generateWarehouseId();
        client.callResetWarehouse(w_id, parameters.districtsPerWarehouse,
            parameters.customersPerDistrict, parameters.newOrdersPerDistrict);
    }

    /** Executes a stock level transaction. */
    public void doStockLevel() throws IOException {
        int threshold = generator.number(TPCCConstants.MIN_STOCK_LEVEL_THRESHOLD,
                                          TPCCConstants.MAX_STOCK_LEVEL_THRESHOLD);

        client.callStockLevel(generateWarehouseId(), generateDistrict(), threshold);
    }

    /** Executes an order status transaction. */
    public void doOrderStatus() throws IOException {
        int y = generator.number(1, 100);

        if (y <= 60) {
            // 60%: order status by last name
            String cLast = generator
                    .makeRandomLastName(parameters.customersPerDistrict);
            client.callOrderStatus(TPCCConstants.ORDER_STATUS_BY_NAME,
                                   generateWarehouseId(), generateDistrict(), cLast);

        } else {
            // 40%: order status by id
            assert y > 60;
            client.callOrderStatus(TPCCConstants.ORDER_STATUS_BY_ID,
                                   generateWarehouseId(), generateDistrict(), generateCID());
        }
    }

    /** Executes a delivery transaction. */
    public void doDelivery()  throws IOException {
        int carrier = generator.number(TPCCConstants.MIN_CARRIER_ID,
                                       TPCCConstants.MAX_CARRIER_ID);

        client.callDelivery(generateWarehouseId(), carrier, clock.getDateTime());
    }

    /** Executes a payment transaction. */
    public void doPayment()  throws IOException {
        boolean allow_remote = (parameters.warehouses > 1 && config.payment_multip != false);
        double remote_prob = (config.payment_multip_mix >= 0 ? config.payment_multip_mix : 15) * 10d;
        
        short w_id = generateWarehouseId();
        byte d_id = generateDistrict();

        short c_w_id;
        byte c_d_id;
        if (allow_remote == false || generator.number(1, 1000) <= (1000-remote_prob)) {
            // 85%: paying through own warehouse (or there is only 1 warehouse)
            c_w_id = w_id;
            c_d_id = d_id;
        } else {
            // 15%: paying through another warehouse:
            if (config.warehouse_pairing) {
                c_w_id = generatePairedWarehouse(w_id, parameters.starting_warehouse, parameters.last_warehouse);
            }
            else if (config.payment_multip_remote) {
                c_w_id = (short)generator.numberRemoteWarehouseId(parameters.starting_warehouse, parameters.last_warehouse, (int)w_id);
            } else {
                // select in range [1, num_warehouses] excluding w_id
                c_w_id = (short)generator.numberExcluding(parameters.starting_warehouse, parameters.last_warehouse, w_id);
            }
            assert c_w_id != w_id;
            c_d_id = generateDistrict();
        }
        double h_amount = generator.fixedPoint(2, TPCCConstants.MIN_PAYMENT,
                                                  TPCCConstants.MAX_PAYMENT);
        TimestampType now = clock.getDateTime();

        if (generator.number(1, 100) <= 60) {
            // 60%: payment by last name
            String c_last = generator.makeRandomLastName(parameters.customersPerDistrict);
            client.callPaymentByName(w_id, d_id, h_amount, c_w_id, c_d_id, c_last, now);
        } else {
            // 40%: payment by id
            client.callPaymentById(w_id, d_id, h_amount, c_w_id, c_d_id,
                                   generateCID(), now);
        }
    }

    /** Executes a new order transaction. */
    public void doNewOrder() throws IOException {
        short warehouse_id = generateWarehouseId();
        int ol_cnt = generator.number(TPCCConstants.MIN_OL_CNT, TPCCConstants.MAX_OL_CNT);

        // % of transactions that roll back
        boolean rollback = (generator.number(1, 100) < config.neworder_abort);
        int local_warehouses = 0;
        int remote_warehouses = 0;

        int[] item_id = new int[ol_cnt];
        short[] supply_w_id = new short[ol_cnt];
        int[] quantity = new int[ol_cnt];
        for (int i = 0; i < ol_cnt; ++i) {
            if (rollback && i + 1 == ol_cnt) {
                // LOG.fine("[NOT_ERROR] Causing a rollback on purpose defined in TPCC spec. "
                //     + "You can ignore following 'ItemNotFound' exception.");
                item_id[i] = parameters.num_items + 1;
            } else {
                item_id[i] = generateItemID();
            }

            // 1% of items are from a remote warehouse
            boolean remote = config.neworder_multip && (generator.number(1, 100) == 1);
            if (parameters.warehouses > 1 && remote) {
                short remote_w_id;
                if (config.warehouse_pairing) {
                    remote_w_id = generatePairedWarehouse(warehouse_id, parameters.starting_warehouse, parameters.last_warehouse);
                }
                else if (config.neworder_multip_remote) {
                    remote_w_id = (short)generator.numberRemoteWarehouseId(parameters.starting_warehouse, parameters.last_warehouse, (int) warehouse_id);
                }
                else {
                    remote_w_id = (short)generator.numberExcluding(parameters.starting_warehouse, parameters.last_warehouse, (int) warehouse_id);
                }
                supply_w_id[i] = remote_w_id;
                if (supply_w_id[i] != warehouse_id) remote_warehouses++;
                else local_warehouses++;
            } else {
                supply_w_id[i] = warehouse_id;
                local_warehouses++;
            }

            quantity[i] = generator.number(1, TPCCConstants.MAX_OL_QUANTITY);
        }
        // Whether to force this transaction to be multi-partitioned
        if (parameters.warehouses > 1 && config.neworder_multip == true && config.neworder_multip_mix > 0) {
            // First force the entire thing to be single-partitioned
            for (int idx = 0; idx < ol_cnt; idx++) {
                supply_w_id[idx] = warehouse_id;
            } // FOR

            // Then check whether we should flip a random SUPPLY_W_ID to be remote
            if (generator.number(1, 1000) <= (config.neworder_multip_mix*100)) {
                if (trace.val) LOG.trace("Forcing Multi-Partition NewOrder Transaction");
                // Flip a random one
                int idx = generator.number(0, ol_cnt-1);
                short remote_w_id;
                if (config.warehouse_pairing) {
                    remote_w_id = generatePairedWarehouse(warehouse_id, parameters.starting_warehouse, parameters.last_warehouse);
                }
                else if (config.neworder_multip_remote) {
                	remote_w_id = (short)generator.numberRemoteWarehouseId(parameters.starting_warehouse, parameters.last_warehouse, (int) warehouse_id);
                } else {
                	remote_w_id = (short)generator.numberExcluding(parameters.starting_warehouse, parameters.last_warehouse, (int) warehouse_id);
                }
                supply_w_id[idx] = remote_w_id;
                if (supply_w_id[idx] != warehouse_id) remote_warehouses++;
                else local_warehouses++;
            }
        }
        // Prevent aborts
        if (rollback && (
            (remote_warehouses > 0 && config.neworder_abort_no_multip) ||
            (remote_warehouses == 0 && config.neworder_abort_no_singlep))
           ) {
            item_id[ol_cnt-1] = generateItemID();
        }

        if (trace.val)
            LOG.trace("newOrder(W_ID=" + warehouse_id + ") -> [" +
                      "local_warehouses=" + local_warehouses + ", " +
                      "remote_warehouses=" + remote_warehouses + "]");

        TimestampType now = clock.getDateTime();
        client.callNewOrder(rollback, config.noop, warehouse_id, generateDistrict(), generateCID(),
                            now, item_id, supply_w_id, quantity);
    }

    /**
     * Selects and executes a transaction at random. The number of new order
     * transactions executed per minute is the official "tpmC" metric. See TPC-C
     * 5.4.2 (page 71).
     *
     * @return the transaction that was executed..
     */
    public int doOne(TPCCClient.Transaction t) throws IOException {
        // This is not strictly accurate: The requirement is for certain
        // *minimum* percentages to be maintained. This is close to the right
        // thing, but not precisely correct. See TPC-C 5.2.4 (page 68).
       if (config.noop || config.neworder_only) {
           doNewOrder();
           return TPCCClient.Transaction.NEW_ORDER.ordinal();
        }
        
        switch (t) {
            case STOCK_LEVEL:
                doStockLevel();
                break;
            case DELIVERY:
                doDelivery();
                break;
            case ORDER_STATUS:
                doOrderStatus();
                break;
            case PAYMENT:
                doPayment();
                break;
            case NEW_ORDER:
                doNewOrder();
                break;
            default:
                assert(false) : "Unexpected transaction " + t;
        }
        return (t.ordinal());
    }
}
