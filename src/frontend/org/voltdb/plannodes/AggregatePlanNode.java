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

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

/**
 *
 */
public class AggregatePlanNode extends AbstractPlanNode {

    public enum Members {
        AGGREGATE_COLUMNS,
        AGGREGATE_TYPE,
        AGGREGATE_NAME,
        AGGREGATE_GUID,
        AGGREGATE_OUTPUT_COLUMN,
        GROUPBY_COLUMNS;
    }

    // NOTE: I'm not really keen on how this is all laid out, but it's just
    //     good enough for what we need in TPC-C for now...
    private List<ExpressionType> m_aggregateTypes = new ArrayList<ExpressionType>();

    // a list of column offsets/indexes not plan column guids.
    private List<Integer> m_aggregateOutputColumns = new ArrayList<Integer>();
    // a list of the names of the columns that are being aggregated
    private List<String> m_aggregateColumnNames = new ArrayList<String>();
    // a list of the GUIDs for the columns that are being aggregated
    private List<Integer> m_aggregateColumnGuids = new ArrayList<Integer>();

    private List<Integer> m_groupByColumns = new ArrayList<Integer>();
    private List<Integer> m_groupByColumnGuids = new ArrayList<Integer>();
    private List<String> m_groupByColumnNames = new ArrayList<String>();

    public AggregatePlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }
    
    @Override
    public Object clone(boolean clone_children, boolean clone_inline) throws CloneNotSupportedException {
        AggregatePlanNode clone = (AggregatePlanNode)super.clone(clone_children, clone_inline);
        
        clone.m_aggregateTypes = new ArrayList<ExpressionType>(this.m_aggregateTypes);
        clone.m_aggregateOutputColumns = new ArrayList<Integer>(this.m_aggregateOutputColumns);
        clone.m_aggregateColumnNames = new ArrayList<String>(this.m_aggregateColumnNames);
        clone.m_aggregateColumnGuids = new ArrayList<Integer>(this.m_aggregateColumnGuids);
        clone.m_groupByColumns = new ArrayList<Integer>(this.m_groupByColumns);
        clone.m_groupByColumnGuids = new ArrayList<Integer>(this.m_groupByColumnGuids);
        clone.m_groupByColumnNames = new ArrayList<String>(this.m_groupByColumnNames);
        
        return (clone);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.AGGREGATE;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // We need to have an aggregate type and column
        // We're not checking that it's a valid ExpressionType because this plannode is a temporary hack
        //
        if (m_aggregateTypes.size() != m_aggregateColumnNames.size() ||
            m_aggregateColumnNames.size() != m_aggregateOutputColumns.size())
        {
            throw new Exception("ERROR: Mismatched number of aggregate expression column attributes for PlanNode '" + this + "'");
        } else if (m_aggregateTypes.isEmpty()|| m_aggregateTypes.contains(ExpressionType.INVALID)) {
            throw new Exception("ERROR: Invalid Aggregate ExpressionType or No Aggregate Expression types for PlanNode '" + this + "'");
        } else if (m_aggregateColumnNames.isEmpty()) {
            throw new Exception("ERROR: No Aggregate Columns for PlanNode '" + this + "'");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrayList<Integer> createOutputColumns(Database db, ArrayList<Integer> input) {
        // columns are created during plan node construction
        assert(m_outputColumns.size() > 0);
        return (ArrayList<Integer>)m_outputColumns.clone();
    }

    public void appendOutputColumn(PlanColumn colInfo) {
        m_outputColumns.add(colInfo.guid());
    }
    /**
     * @return The list of output column indexes that each aggregate outputs to
     */
    public List<Integer> getAggregateOutputColumns() {
        return m_aggregateOutputColumns;
    }

    /**
     * @return The type of aggregation for each aggregate column
     */
    public List<ExpressionType> getAggregateTypes() {
        return m_aggregateTypes;
    }
    /**
     * @param aggregate_types
     */
    public void setAggregateTypes(List<ExpressionType> aggregate_types) {
        m_aggregateTypes = aggregate_types;
    }

    /**
     * @return the aggregate column GUIDs (aggregated input col GUID)
     */
    public List<Integer> getAggregateColumnGuids() {
        return m_aggregateColumnGuids;
    }
    /**
     * @param aggregate_column_guids
     */
    public void setAggregateColumnGuids(List<Integer> aggregate_column_guids) {
        m_aggregateColumnGuids = aggregate_column_guids;
    }

    /**
     * @return the aggregate_column_name
     */
    public List<String> getAggregateColumnNames() {
        return m_aggregateColumnNames;
    }
    /**
     * @param aggregate_column_names
     */
    public void setAggregateColumnNames(List<String> aggregate_column_names) {
        m_aggregateColumnNames = aggregate_column_names;
    }

    /**
     * @return Names of the input column that maps to the output column.
     */
    public List<String> getOutputColumnInputAliasNames() {
        return m_aggregateColumnNames;
    }

    /**
     * @return the groupby_columns
     */
    public List<Integer> getGroupByColumnOffsets() {
        return m_groupByColumns;
    }
    
    /**
     * GroupBy PlanColumn GUIDs 
     * @return
     */
    public List<Integer> getGroupByColumnGuids() {
        return m_groupByColumnGuids;
    }

    public void appendGroupByColumn(PlanColumn colInfo) {
        m_groupByColumnGuids.add(colInfo.guid());
    }

    /**
     * @return the groupby_column_names
     */
    public List<String> getGroupByColumnNames() {
        return m_groupByColumnNames;
    }
    /**
     * @param groupby_column_names
     */
    public void setGroupByColumnNames(List<String> groupby_column_names) {
        m_groupByColumnNames = groupby_column_names;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key("AGGREGATE_COLUMNS");
        stringer.array();
        for (int ii = 0; ii < m_aggregateTypes.size(); ii++) {
            stringer.object();
            stringer.key(Members.AGGREGATE_TYPE.name()).value(m_aggregateTypes.get(ii).name());
            stringer.key(Members.AGGREGATE_NAME.name()).value(m_aggregateColumnNames.get(ii));
            stringer.key(Members.AGGREGATE_GUID.name()).value(m_aggregateColumnGuids.get(ii));
            stringer.key(Members.AGGREGATE_OUTPUT_COLUMN.name()).value(m_aggregateOutputColumns.get(ii));
            stringer.endObject();
        }
        stringer.endArray();

        if (!m_groupByColumnGuids.isEmpty())
        {   
            stringer.key(Members.GROUPBY_COLUMNS.name()).array();
            for (int i = 0; i < m_groupByColumnGuids.size(); i++) {
                PlanColumn column = m_context.get(m_groupByColumnGuids.get(i));
                column.toJSONString(stringer);
            }
            stringer.endArray();
        }
    }
    
    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        JSONArray aggregateColumns = obj.getJSONArray(Members.AGGREGATE_COLUMNS.name());
        for (int ii = 0; ii < aggregateColumns.length(); ii++) {
            JSONObject aggregateColumn = aggregateColumns.getJSONObject(ii);
            m_aggregateTypes.add(ExpressionType.valueOf(aggregateColumn.getString(Members.AGGREGATE_TYPE.name())));
            m_aggregateColumnNames.add(aggregateColumn.getString(Members.AGGREGATE_NAME.name()));
            m_aggregateColumnGuids.add(aggregateColumn.getInt(Members.AGGREGATE_GUID.name()));
            m_aggregateOutputColumns.add(aggregateColumn.getInt(Members.AGGREGATE_OUTPUT_COLUMN.name()));
        }
        
        try {
            JSONArray groupbyColumnGuids = obj.getJSONArray(Members.GROUPBY_COLUMNS.name());
            for (int ii = 0; ii < groupbyColumnGuids.length(); ii++) {
                JSONObject jsonObject = groupbyColumnGuids.getJSONObject(ii);
                PlanColumn column = PlanColumn.fromJSONObject(jsonObject, db);
                m_groupByColumnGuids.add(column.guid());
            }
        } catch (JSONException e) {
            //okay not to be there.
        }
    }
}
