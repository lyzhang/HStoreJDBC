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

import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStore;

/**
 * <p>The TransactionIdManager creates Transaction ids that
 * get assigned to VoltDB timestamps. A transaction id contains
 * three fields, the time of creation, a counter to ensure local
 * ordering, and the siteid of the generating site.</p>
 *
 * <p>This class also contains methods to examine the embedded values of
 * transaction ids.</p>
 *
 * <p>If the clocks of two different machines are reasonably in sync,
 * txn ids created at the same time on different machines will be reasonably
 * close in value. Thus transaction ids can be used for a global ordering.</p>
 *
 */
public class TransactionIdManager {
    private static final Logger LOG = Logger.getLogger(TransactionIdManager.class);
    
    // bit sizes for each of the fields in the 64-bit id
    // note, these add up to 63 bits to make dealing with
    // signed / unsigned conversions easier.
    static final long TIMESTAMP_BITS = 40;
    static final long COUNTER_BITS = 13;
    static final long INITIATORID_BITS = 10;
    
    static final int DRIFT_CHECK = 5; // ms

    // VOLT_EPOCH holds the time in millis since 1/1/2008 at 12am.
    // The current time - VOLT_EPOCH should fit nicely in 40 bits
    // of memory.
    static final long VOLT_EPOCH = getEpoch();
    public static long getEpoch() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(0);
        c.set(2010, 0, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.ZONE_OFFSET, 0);
        c.set(Calendar.DST_OFFSET, 0);
        long retval = c.getTimeInMillis();
        return retval;
    }

    // maximum values for the fields
    // used for bit-shifts and error checking
    static final long TIMESTAMP_MAX_VALUE = (1L << TIMESTAMP_BITS) - 1L;
    static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    static final long INITIATORID_MAX_VALUE = (1L << INITIATORID_BITS) - 1L;

    // the local siteid
    long initiatorId;
    // the time of the previous txn id generation
    long lastUsedTime = -1;
    // the number of txns generated during the same value
    // for System.currentTimeMillis()
    long counterValue = 0;

    // remembers the last txn generated
    Long lastTxnId = 0l;
    
    long time_delta = 0L;

    /**
     * Initialize the TransactionIdManager for this site
     * @param initiatorId The siteId of the current site.
     */
    public TransactionIdManager(int initiatorId) {
        this.initiatorId = initiatorId;
    }

    /**
     * Generate a unique id that contains a timestamp, a counter
     * and a siteid packed into a 64-bit long value. Subsequent calls
     * to this method will return strictly larger long values.
     * @return The newly generated transaction id.
     */
    public Long getNextUniqueTransactionId() {
        long currentTime = 0;
        long currentCounter = 0;
        
        synchronized (this) {
            currentTime = System.currentTimeMillis();
            if (currentTime == this.lastUsedTime) {
                // increment the counter for this millisecond
                currentCounter = ++this.counterValue;
    
                // handle the case where we've run out of counter values
                // for this particular millisecond (feels unlikely)
                if (this.counterValue > COUNTER_MAX_VALUE) {
                    LOG.warn("TOO MANY TXNS! SPIN LOCK!!!");
                    // spin until the next millisecond
                    while (currentTime == this.lastUsedTime)
                        currentTime = System.currentTimeMillis();
                    // reset the counter and lastUsedTime for the new millisecond
                    this.lastUsedTime = currentTime;
                    currentCounter = this.counterValue = 0;
                }
            }
            else {
                // reset the counter and lastUsedTime for the new millisecond
                if (currentTime < this.lastUsedTime) {
                    LOG.warn(String.format("Initiator time moved backwards from %d to %d by %d ms!!!",
                                           this.lastUsedTime, currentTime, (this.lastUsedTime - currentTime)));
                    // if the diff is less than 5 ms, wait a bit
                    if ((this.lastUsedTime - currentTime) < DRIFT_CHECK) {
                        int count = 1000;
                        // note, the loop should stop once lastUsedTime is PASSED, not current
                        while ((currentTime <= this.lastUsedTime) && (count-- > 0)) {
                            try {
                                Thread.sleep(this.lastUsedTime - currentTime + 1);
                            } catch (InterruptedException e) {}
                            currentTime = System.currentTimeMillis();
                        }
                        // if the loop above ended because it ran too much
                        if (count < 0) {
                            LOG.error("H-Store was unable to recover after the system time was externally negatively adusted. " +
                                      "It is possible that there is a serious system time or NTP error. ");
                            HStore.crashDB();
                        }
                    }
                    // crash immediately if time has gone backwards by too much
                    else {
                        HStore.crashDB();
                    }
                }
                this.lastUsedTime = currentTime;
                currentCounter = this.counterValue = 0;
            }
        } // SYNCH

        Long newTxnId = new Long(makeIdFromComponents(currentTime + this.time_delta,
                                 currentCounter,
                                 this.initiatorId));
        this.lastTxnId = newTxnId;
        return (newTxnId);
    }

    public static long makeIdFromComponents(long ts, long seqNo, long initiatorId) {
        // compute the time in millis since VOLT_EPOCH
        long txnId = ts - VOLT_EPOCH;
        // verify all fields are the right size
        assert(txnId <= TIMESTAMP_MAX_VALUE);
        assert(seqNo <= COUNTER_MAX_VALUE);
        assert(initiatorId <= INITIATORID_MAX_VALUE);

        // put this time value in the right offset
        txnId = txnId << (COUNTER_BITS + INITIATORID_BITS);
        // add the counter value at the right offset
        txnId |= seqNo << INITIATORID_BITS;
        // finally add the siteid at the end
        txnId |= initiatorId;

        return txnId;
    }

    /**
     * Given a transaction id, return the time of its creation
     * by examining the embedded timestamp.
     * @param txnId The transaction id value to examine.
     * @return The Date object representing the time this transaction
     * id was created.
     */
    public static Date getDateFromTransactionId(long txnId) {
        long time = txnId >> (COUNTER_BITS + INITIATORID_BITS);
        time += VOLT_EPOCH;
        return new Date(time);
    }

    /**
     * Given a transaction id, return the time of its creation
     * by examining the embedded timestamp.
     * @param txnId The transaction id value to examine.
     * @return The integer representing the time this transaction
     * id was created.
     */
    public static long getTimestampFromTransactionId(long txnId) {
        long time = txnId >> (COUNTER_BITS + INITIATORID_BITS);
        time += VOLT_EPOCH;
        return time;
    }

    /**
     * Given a transaction id, return the embedded site id.
     * @param txnId The transaction id value to examine.
     * @return The site id embedded within the transaction id.
     */
    public static long getInitiatorIdFromTransactionId(long txnId) {
        return txnId & INITIATORID_MAX_VALUE;
    }

    public static long getSequenceNumberFromTransactionId(long txnId) {
        long seq = txnId >> INITIATORID_BITS;
        seq = seq & COUNTER_MAX_VALUE;
        return seq;
    }

    /**
     * Get the last txn id generated.
     * @return The last txn id generated.
     */
    public Long getLastTxnId() {
        return lastTxnId;
    }

    public long getLastUsedTime() {
        return lastUsedTime;
    }

    /**
     * This should not be invoked directly by anybody else at runtime
     * @param delta
     */
    public void setTimeDelta(long delta) {
        this.time_delta = delta;
    }
    
    /**
     * Get a string representation of the TxnId
     */
    public static String toString(long txnId) {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("Timestamp: ").append(getTimestampFromTransactionId(txnId));
        sb.append(".").append(getSequenceNumberFromTransactionId(txnId));
        sb.append(" InititatorId: ").append(getInitiatorIdFromTransactionId(txnId));
        return sb.toString();
    }

    public static String toBitString(long txnId) {
        String retval = "";
        long mask = 0x8000000000000000L;
        for(int i = 0; i < 64; i++) {
            if ((txnId & mask) == 0) retval += "0";
            else retval += "1";
            mask >>>= 1;
        }
        return retval;
    }
}
