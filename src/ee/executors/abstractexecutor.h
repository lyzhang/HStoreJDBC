/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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

#ifndef HSTORENODEABSTRACTEXECUTOR_H
#define HSTORENODEABSTRACTEXECUTOR_H

#include <vector>
#include "common/common.h"
#include "common/valuevector.h"
#include "common/executorcontext.hpp"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/ReadWriteTracker.h"
#include "plannodes/abstractplannode.h"
#include "catalog/database.h"

namespace voltdb {

class VoltDBEngine;
class ExecutorContext;
class ReadWriteTracker;

/**
 * AbstractExecutor provides the API for initializing and invoking executors.
 */
class AbstractExecutor {
  public:
    virtual ~AbstractExecutor();


    /** Executors are initialized once when the catalog is loaded */
    bool init(VoltDBEngine*, const catalog::Database *catalog_db, int* tempTableMemoryInBytes);

    /** Invoke a plannode's associated executor */
    bool execute(const NValueArray &params, ReadWriteTracker *tracker);

    /**
     * Returns true if the output table for the plannode must be cleaned up
     * after p_execute().  <b>Default is false</b>. This should be overriden in
     * the receive executor since this is the only place we need to clean up the
     * output table.
     */
    virtual bool needsPostExecuteClear() { return false; }
    
    /** Invoked after all executors in a PF have finished.**/
    void postExecute();

    inline bool forceTupleCount() const { return (this->force_send_tuple_count); }

    /**
     * Returns the plannode that generated this executor.
     */
    inline AbstractPlanNode* getPlanNode() { return abstract_node; }
    
  protected:
    AbstractExecutor(VoltDBEngine *engine, AbstractPlanNode *abstract_node) {
        this->abstract_node = abstract_node;
        tmp_output_table = NULL;
        this->force_send_tuple_count = false;
    }

    /** Concrete executor classes implement initialization in p_init() */
    virtual bool p_init(AbstractPlanNode*, const catalog::Database *catalog_db, int* tempTableMemoryInBytes) = 0;

    /** Concrete executor classes impelmenet execution in p_execute() */
    virtual bool p_execute(const NValueArray &params, ReadWriteTracker *tracker) = 0;

    /**
     * Returns true if the output table for the plannode must be
     * cleared before p_execute().  <b>Default is true (clear each
     * time)</b>. Override this method if the executor receives a
     * plannode instance that must not be cleared.
     * @param abstract_node the plannode about to be executed in p_execute()
     * @return true if output table must be cleared; false otherwise.
     */
    virtual bool needsOutputTableClear() { return true; };

    // execution engine owns the plannode allocation.
    AbstractPlanNode* abstract_node;
    TempTable *tmp_output_table;
    ExecutorContext *executor_context;

    // cache to avoid runtime virtual function call
    bool needs_outputtable_clear_cached;
    
    // pointer to a counter that is shared among all executors for a given fragment
    // represents bytes used by temp tables in aggregate
    //int* m_tempTableMemoryInBytes;
    
    // PAVLO: If this is set to true, then we won't execute the plan
    // node and will force the EE to send back the # of tuples modified
    bool force_send_tuple_count;
};

/**
 * An Executor that modifies existing data, i.e. UPDATE/INSERT.  As
 * such kind of executor has to preserve the existing data,
 * needsOutputTableClear() always returns false. (RTB: this is a
 * confusing explanation - the targettable is modified, not the output
 * table. In reality, update and insert set outputtable to the
 * inputtable. Obviously clearing the input table before executing
 * would be non-sensical, so this setting of needsOutputClear() is
 * valid. But why set output <- input in the first place?)
 */
class OperationExecutor : public AbstractExecutor {
  public:
    virtual ~OperationExecutor() {}

  protected:
    OperationExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) : AbstractExecutor(engine, abstract_node) {}
    virtual bool needsOutputTableClear() { return false; };
};

inline bool AbstractExecutor::execute(const NValueArray &params, ReadWriteTracker *tracker) {
    assert (abstract_node);
    VOLT_TRACE("Starting execution of plannode(id=%d)...", abstract_node->getPlanNodeId());

    if (tmp_output_table) {
        VOLT_TRACE("Clearing output table...");
        tmp_output_table->deleteAllTuplesNonVirtual(false);
    }

    // run the executor
    return this->p_execute(params, tracker);
}

}

#endif
