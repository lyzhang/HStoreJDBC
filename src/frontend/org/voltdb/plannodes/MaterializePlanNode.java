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
public class MaterializePlanNode extends ProjectionPlanNode {

    public enum Members {
        BATCHED;
    }

    private boolean m_batched = false;

    /**
     * @param id
     */
    public MaterializePlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.MATERIALIZE;
    }

    public void setBatched(boolean batched) {
        m_batched = batched;
    }

    public boolean isBatched() {
        return m_batched;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.BATCHED.name()).value(m_batched);
    }
    
    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        super.loadFromJSONObject(obj, db);
        m_batched = obj.getBoolean(Members.BATCHED.name());
    }
}
