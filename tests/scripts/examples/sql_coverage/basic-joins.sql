-- Run the basic-template against the default tables using two table joins

{@insert_vals = "_value[id], _value[string], _value[int32], _value[float]"}
{@from_tables = "_table, _table"}
{@cmp_type = "_value[int:0,100]"}
{@assign_col = "NUM"}
{@assign_type = "_value[int:0,100]"}

<basic-template.sql>
