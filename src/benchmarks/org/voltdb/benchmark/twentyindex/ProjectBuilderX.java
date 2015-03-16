/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.benchmark.twentyindex;

import java.net.URL;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.twentyindex.procedures.Insert;
import org.voltdb.compiler.VoltProjectBuilder;

public class ProjectBuilderX extends VoltProjectBuilder {

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> m_procedures[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        Insert.class,
    };

    public static final Class<?> m_supplementalClasses[] = new Class<?>[] {
        ClientBenchmark.class,
        ProjectBuilderX.class
    };

    public static final URL m_ddlURL = ProjectBuilderX.class.getResource("ddl.sql");

    public static String m_partitioning[][] = new String[][] {
        {"TABLE1", "MAINID"},
        {"TABLE2", "MAINID"},
        {"TABLE3", "MAINID"},
    };
    
    public ProjectBuilderX() {
        super("twenty");
    }

    @Override
    public void addAllDefaults() {
        addProcedures(m_procedures);
        for (String partitionInfo[] : m_partitioning) {
            addTablePartitionInfo(partitionInfo[0], partitionInfo[1]);
        }
        addSchema(m_ddlURL);
        addSupplementalClasses(m_supplementalClasses);
    }
}
