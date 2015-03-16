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
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlannerContext;

/**
 *
 */
public abstract class AbstractOperationPlanNode extends AbstractPlanNode {

    public enum Members {
        TARGET_TABLE_NAME;
    }

    // The target table is the table that the plannode wants to perform some operation on.
    private String m_targetTableName = "";

    protected AbstractOperationPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    protected String debugInfo(String spacer) {
        return spacer + "TargetTableName[" + m_targetTableName + "]\n";
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof AbstractOperationPlanNode) == false) {
            return (false);
        }
        AbstractOperationPlanNode other = (AbstractOperationPlanNode)obj;
        if (this.m_targetTableName.equals(other.m_targetTableName) == false) return (false);
        return super.equals(obj);
    }
    
    @Override
    public void validate() throws Exception {
        super.validate();

        // All Operation nodes need to have a target table
        if (m_targetTableName == null) {
            throw new Exception("ERROR: The Target TableId is null for PlanNode '" + this + "'");
        }
    }

    /**
     * @return the target_table_name
     */
    public final String getTargetTableName() {
        return m_targetTableName;
    }

    /**
     * @param target_table_name the target_table_name to set
     */
    public final void setTargetTableName(final String target_table_name) {
        m_targetTableName = target_table_name;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.TARGET_TABLE_NAME.name()).value(m_targetTableName);
    }
    
    @Override
    protected void loadFromJSONObject(final JSONObject obj, Database db) throws JSONException {
        m_targetTableName = obj.getString(Members.TARGET_TABLE_NAME.name());
        if (db != null || m_targetTableName.equals("")) {
            Table table = db.getTables().get(m_targetTableName);
            if (table == null) {
                throw new JSONException("Unable to retrieve catalog object for table '" + m_targetTableName + "'");
            }
        }
    }
}
