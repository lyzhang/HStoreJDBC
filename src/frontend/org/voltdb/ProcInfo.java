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

import java.lang.annotation.*;

import edu.brown.catalog.special.NullProcParameter;

/**
 * Annotates a stored procedure with information needed by the stored
 * procedure compiler.
 *
 * @see VoltProcedure
 * @see ProcInfoData
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProcInfo {

    /**
     * Information needed to direct this procedure call to the proper partition(s).
     * @return A string of the form "table.column: parametername" that maps columns
     * in a table to a parameter value. This is required for a single-sited procedure.
     */
    String partitionInfo() default "";
    
    /**
     * Explicitly define what ProcParameter to use to route transactions
     */
    int partitionParam() default NullProcParameter.PARAM_IDX;

    /**
     * Is the procedure single sited? This information is hard to determine if there
     * is more than one SQL statement.
     * @return True if all statements run on the same partition always, false otherwise.
     */
    boolean singlePartition() default false;
    
    /**
     * Is the procedure a map/reduce job.
     * It must have a map and reduce function
     */
    String mapInputQuery() default "";
    String reduceInputQuery() default "";
}
