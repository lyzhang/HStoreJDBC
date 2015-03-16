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

package org.voltdb.plannodes;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.PlanNodeType;

/**
 *
 */
public class LimitPlanNode extends AbstractPlanNode {

    public enum Members {
        OFFSET,
        LIMIT,
        OFFSET_PARAM_IDX,
        LIMIT_PARAM_IDX;
    }

    private int m_offset = 0;
    private int m_limit = -1;

    // -1 also interpreted by EE as uninitialized
    private long m_limitParameterId = -1;
    private long m_offsetParameterId = -1;

    /**
     * @param id
     */
    public LimitPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.LIMIT;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Limit Amount
        if (m_limit < 0) {
            throw new Exception("ERROR: The limit size is negative [" + m_limit + "]");
        } else if (m_offset < 0) {
            throw new Exception("ERROR: The offset amount  is negative [" + m_offset + "]");
        }
    }

    /**
     * @return the limit
     */
    public int getLimit() {
        return m_limit;
    }
    /**
     * @param limit the limit to set
     */
    public void setLimit(int limit) {
        m_limit = limit;
    }
    /**
     * @return the offset
     */
    public int getOffset() {
        return m_offset;
    }
    /**
     * @param offset the offset to set
     */
    public void setOffset(int offset) {
        m_offset = offset;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.OFFSET.name()).value(m_offset);
        stringer.key(Members.LIMIT.name()).value(m_limit);
        stringer.key(Members.OFFSET_PARAM_IDX.name()).value(m_offsetParameterId);
        stringer.key(Members.LIMIT_PARAM_IDX.name()).value(m_limitParameterId);
    }

    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        m_offset = obj.getInt(Members.OFFSET.name());
        m_limit = obj.getInt(Members.LIMIT.name());
    }

    public void setLimitParameterIndex(long limitParameterId) {
        m_limitParameterId = limitParameterId;
    }

    public void setOffsetParameterIndex(long offsetParameterId) {
        m_offsetParameterId = offsetParameterId;
    }

}
