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


package org.voltdb.benchmark.blobtorture;

import org.voltdb.benchmark.blobtorture.procedures.*;
import org.voltdb.compiler.VoltProjectBuilder;

import edu.brown.api.BenchmarkComponent;

import java.net.URL;

public class BlobTortureProjectBuilder extends VoltProjectBuilder {

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_clientClass = BlobTortureClient.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = null;
    
    private static final URL ddlURL =
        BlobTortureProjectBuilder.class.getResource("blobtorture-ddl.sql");

    
    public BlobTortureProjectBuilder() {
        super("blob");
    }
    
    @Override
    public void addAllDefaults() {
        addProcedures(InsertBlob.class, SelectBlob.class);
        addSchema(ddlURL);
        addTablePartitionInfo("BLOBS", "BLOB_ID");
    }

}
