/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.smallbank.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.TheHashinator;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.SmallBankConstants;

/**
 * Amalgamate Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
@ProcInfo (
    partitionParam=0
)
public class Amalgamate extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(Amalgamate.class);
    
    // 2013-05-05
    // In the original version of the benchmark, this is suppose to be a look up
    // on the customer's name. We don't have fast implementation of replicated 
    // secondary indexes, so we'll just ignore that part for now.
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public final SQLStmt ZeroCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = 0.0 " +
        " WHERE custid = ?"
    );
    
    public VoltTable run(long acctId0, long acctId1) {
        // Get Account Information
        voltQueueSQL(GetAccount, acctId1);
        voltQueueSQL(GetAccount, acctId0);
        final VoltTable acctResults[] = voltExecuteSQL();
        if (acctResults[0].getRowCount() != 1) {
            String msg = "Invalid account '" + acctId0 + "'\n" + acctResults[0]; 
            LOG.error(this.getTransactionState() + " - " + msg + " / hash=" + TheHashinator.hashToPartition(acctId0));
            throw new VoltAbortException(msg);
        }
        if (acctResults[1].getRowCount() != 1) {
            String msg = "Invalid account '" + acctId1 + "'\n" + acctResults[1];
            LOG.error(this.getTransactionState() + " - " + msg + " / hash=" + TheHashinator.hashToPartition(acctId1));
            throw new VoltAbortException(msg);
        }
        
        // Get Balance Information
        voltQueueSQL(GetSavingsBalance, acctId0);
        voltQueueSQL(GetCheckingBalance, acctId1);
        final VoltTable balResults[] = voltExecuteSQL();
        if (balResults[0].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId0);
            LOG.error(msg);
            throw new VoltAbortException(msg);
        }
        if (balResults[1].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       acctId0);
            LOG.error(msg);
            throw new VoltAbortException(msg);
        }
        balResults[0].advanceRow();
        balResults[1].advanceRow();
        double total = balResults[0].getDouble(0) + balResults[1].getDouble(0);
        // assert(total >= 0);

        // Update Balance Information
        voltQueueSQL(ZeroCheckingBalance, acctId0);
        voltQueueSQL(UpdateSavingsBalance, total, acctId1);
        
                
//        if (balance < 0) {
//            String msg = String.format("Negative %s balance for customer #%d",
//                                       SmallBankConstants.TABLENAME_SAVINGS, 
//                                       acctId);
//            throw new VoltAbortException(msg);
//        }
//        
//        voltQueueSQL(UpdateSavingsBalance, amount);
//        results = voltExecuteSQL(true);
//        return (results[0]);
        return (null);
    }
}