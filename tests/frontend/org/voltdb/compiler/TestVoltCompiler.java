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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.ProcInfoData;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.*;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.types.ConstraintType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.JarReader;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;

public class TestVoltCompiler extends BaseTestCase {
    
    String nothing_jar;
    String testout_jar;

    final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");
    
    @Override
    public void setUp() {
        nothing_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "nothing.jar";
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        File njar = new File(nothing_jar);
        njar.delete();
        File tjar = new File(testout_jar);
        tjar.delete();
    }
    
    public void testSnapshotSettings() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        VoltProjectBuilder builder = new VoltProjectBuilder("testvoltcompiler");

        builder.addProcedures(org.voltdb.compiler.procedures.TPCCTestProc.class);
        builder.setSnapshotSettings("32m", 5, "/tmp", "woobar");
        builder.addSchema(schemaPath);
        try {
            assertTrue(builder.compile("/tmp/snapshot_settings_test.jar"));
            final String catalogContents =
                JarReader.readFileFromJarfile("/tmp/snapshot_settings_test.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);
            SnapshotSchedule schedule =
                cat.getClusters().get("cluster").getDatabases().
                    get("database").getSnapshotschedule().get("default");
            assertEquals(32, schedule.getFrequencyvalue());
            assertEquals("m", schedule.getFrequencyunit());
            assertEquals("/tmp", schedule.getPath());
            assertEquals("woobar", schedule.getPrefix());
        } finally {
            final File jar = new File("/tmp/snapshot_settings_test.jar");
            jar.delete();
        }
    }

    // TestELTSuite tests most of these options end-to-end; however need to test
    // that a disabled connector is really disabled and that auth data is correct.
    public void testELTSetting() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder("sqltypessuite");
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addProcedures(org.voltdb.regressionsuites.sqltypesprocs.Insert.class);
        project.addTablePartitionInfo("NO_NULLS", "PKEY");
        project.addTablePartitionInfo("ALLOW_NULLS", "PKEY");
        project.addTablePartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addTablePartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addTablePartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addTablePartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addTablePartitionInfo("JUMBO_ROW", "PKEY");
        project.addELT("org.voltdb.elt.processors.RawProcessor", false, null, null);
        project.addELTTable("ALLOW_NULLS", false);   // persistent table
        project.addELTTable("WITH_DEFAULTS", true);  // streamed table
        try {
            assertTrue(project.compile("/tmp/eltsettingstest.jar"));
            final String catalogContents =
                JarReader.readFileFromJarfile("/tmp/eltsettingstest.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);

            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("0");
            assertFalse(connector.getEnabled());

        } finally {
            final File jar = new File("/tmp/eltsettingstest.jar");
            jar.delete();
        }

    }

    public void testBadPath() {
        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");
        final boolean success = compiler.compile("invalidnonsense",
                                                 cluster_config,
                                                 "nothing", System.out, null);

        assertFalse(success);
    }

    public void testInvalidXMLFile() {
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='my schema file.sql' /></schemas>" +
            "<procedures><procedure class='procedures/procs.jar' /></procedures>" +
            "</database>" +
            "</project>";

        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(path, cluster_config,
                                           "nothing", System.out, null);

        assertFalse(success);
    }

    public void testXMLFileWithSchemaError() {
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='my schema file.sql' /></schemas>" +
            "<procedures><procedure class='procedures/procs.jar'/></procedures>" +
            "</database>" +
            "</project>";

        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(path, cluster_config,
                                           "nothing", System.out, null);

        assertFalse(success);
    }

    public void testXMLFileWithWrongDBName() {
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='mydb1'>" +
            "<schemas>" +
            "<schema path='my schema file.sql' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='procedures/procs.jar' />" +
            "</procedures>" +
            "</database>" +
            "</project>";

        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(path, cluster_config,
                                                 "nothing", System.out, null);

        assertFalse(success);
    }

    public void testBadClusterConfig() throws IOException {
        String simpleSchema =
            "create table books (cash integer default 23, title varchar default 'foo', PRIMARY KEY(cash));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();

        String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<!-- xml comment check -->" +
            "<database name='database'>" +
            "<!-- xml comment check -->" +
            "<schema path='" + schemaPath + "' />" +
            "<!-- xml comment check -->" +
            "<procedure class='org.voltdb.compiler.procedures.AddBook' />" +
            "<!-- xml comment check -->" +
            "</database>" +
            "<!-- xml comment check -->" +
            "</project>";

        File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        String projectPath = projectFile.getPath();

        VoltCompiler compiler = new VoltCompiler();

        // check no hosts
        ClusterConfig cluster_config = new ClusterConfig(0, 1, 0, "localhost");
        boolean success = compiler.compile(projectPath, cluster_config,
                                           "testout.jar", System.out, null);
        assertFalse(success);

        // check no sites-per-hosts
        cluster_config = new ClusterConfig(1, 0, 0, "localhost");
        success = compiler.compile(projectPath, cluster_config,
                                   "testout.jar", System.out, null);
        assertFalse(success);

        File jar = new File("testout.jar");
        jar.delete();
    }

    public void testXMLFileWithDDL() throws IOException {
        final String simpleSchema1 =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";
        // newline inserted to test catalog friendliness
        final String simpleSchema2 =
            "create table books2\n (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile1 = VoltProjectBuilder.writeStringToTempFile(simpleSchema1);
        final String schemaPath1 = schemaFile1.getPath();
        final File schemaFile2 = VoltProjectBuilder.writeStringToTempFile(simpleSchema2);
        final String schemaPath2 = schemaFile2.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<!-- xml comment check -->" +
            "<database name='database'>" +
            "<!-- xml comment check -->" +
            "<schemas>" +
            "<!-- xml comment check -->" +
            "<schema path='" + schemaPath1 + "' />" +
            "<schema path='" + schemaPath2 + "' />" +
            "<!-- xml comment check -->" +
            "</schemas>" +
            "<!-- xml comment check -->" +
            "<procedures>" +
            "<!-- xml comment check -->" +
            "<procedure class='org.voltdb.compiler.procedures.AddBook' />" +
            "<procedure class='Foo'>" +
            "<sql>select * from books;</sql>" +
            "</procedure>" +
            "</procedures>" +
            "<!-- xml comment check -->" +
            "</database>" +
            "<!-- xml comment check -->" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testDDLWithNoLengthString() throws IOException {
        final String simpleSchema1 =
            "create table books (cash integer default 23, title varchar default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema1);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='org.voltdb.compiler.procedures.AddBook' />" +
            "<procedure class='Foo'>" +
            "<sql>select * from books;</sql>" +
            "</procedure>" +
            "</procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);
        assertFalse(success);
    }

    public void testNullablePartitionColumn() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook'/></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);

        assertFalse(success);

        boolean found = false;
        for (final VoltCompiler.Feedback fb : compiler.m_errors) {
            if (fb.message.indexOf("Partition column") > 0)
                found = true;
        }
        assertTrue(found);
    }

    public void testXMLFileWithBadDDL() throws IOException {
        final String simpleSchema =
            "create table books (id integer default 0, strval varchar(33000) default '', PRIMARY KEY(id));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);

        assertFalse(success);
    }

    // NOTE: TPCCTest proc also tests whitespaces regressions in SQL literals
    public void testWithTPCCDDL() {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);

        assertTrue(success);

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testSeparateCatalogCompilation() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler1 = new VoltCompiler();
        final VoltCompiler compiler2 = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final Catalog catalog = compiler1.compileCatalog(projectPath, cluster_config);
        final String cat1 = catalog.serialize();
        final boolean success = compiler2.compile(projectPath, cluster_config,
                                                  "testout.jar", System.out, null);
        final String cat2 = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        assertTrue(success);
        assertTrue(cat1.compareTo(cat2) == 0);

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testDDLTableTooManyColumns() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TestVoltCompiler.class.getResource("toowidetable-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);
        assertFalse(success);

        boolean found = false;
        for (final VoltCompiler.Feedback fb : compiler.m_errors) {
            if (fb.message.startsWith("Table MANY_COLUMNS has"))
                found = true;
        }
        assertTrue(found);
    }

    public void testExtraFilesExist() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);
        assertTrue(success);

        final String sql = JarReader.readFileFromJarfile("testout.jar", "tpcc-ddl.sql");
        assertNotNull(sql);

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testXMLFileWithELEnabled() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            " <database name='database'>" +
            "  <schemas><schema path='" + schemaPath + "' /></schemas>" +
            "  <procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "  <exports><connector class='org.voltdb.VerticaLoader'> " +
            "             <tables><table name='foo' exportonly='true'/></tables>" +
            "           </connector>" +
            "  </exports>" +
            " </database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                "testout.jar", System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();
        //System.out.println("PRINTING Catalog 1");
        //System.out.println(c1.serialize());

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testOverrideProcInfo() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 not null, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final ProcInfoData info = new ProcInfoData();
        info.singlePartition = true;
        info.partitionInfo = "BOOKS.CASH: 0";
        final Map<String, ProcInfoData> overrideMap = new HashMap<String, ProcInfoData>();
        overrideMap.put("AddBook", info);

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out,
                                                 overrideMap);

        assertTrue(success);

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        final Database db = c2.getClusters().get("cluster").getDatabases().get("database");
        final Procedure addBook = db.getProcedures().get("AddBook");
        assertEquals(true, addBook.getSinglepartition());

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testBadStmtProcName() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 not null, title varchar default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='@Foo'><sql>select * from books;</sql></procedure></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out,
                                                 null);

        assertFalse(success);
    }

    public void testGoodStmtProcName() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 not null, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='Foo'><sql>select * from books;</sql></procedure></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out,
                                                 null);

        assertTrue(success);

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testMaterializedView() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(32) default 'foo', PRIMARY KEY(cash));\n" +
            "create view matt (title, num, foo) as select title, count(*), sum(cash) from books group by title;";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, cluster_config,
                                                 "testout.jar", System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testForeignKeys() {
        String schemaPath = "";
        try {
            final URL url = new TPCCProjectBuilder().getDDLURL(true);
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final Catalog catalog = compiler.compileCatalog(projectPath, cluster_config);
        assertNotNull(catalog);

        // Now check that CUSTOMER correctly references DISTRICT
        //  (1) Make sure CUSTOMER has a fkey constraint
        //  (2) Make sure that each source column in CUSTOMER points to the constraint
        //  (3) Make sure that the fkey constraint points to DISTRICT
        final Database catalog_db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertNotNull(catalog_db);

        final Table cust_table = catalog_db.getTables().get("CUSTOMER");
        assertNotNull(cust_table);
        final Table dist_table = catalog_db.getTables().get("DISTRICT");
        assertNotNull(dist_table);

        // In the code below, we will refer to the column that is pointed to by another column
        // in the dependency as the parent, and the column with the fkey constraint as the child
        boolean found = false;
        for (final Constraint catalog_const : cust_table.getConstraints()) {
            final ConstraintType const_type = ConstraintType.get(catalog_const.getType());
            if (const_type == ConstraintType.FOREIGN_KEY) {
                found = true;
                assertEquals(dist_table, catalog_const.getForeignkeytable());
                assertEquals(catalog_const.getForeignkeycols().size(), 2);

                for (final ColumnRef catalog_parent_colref : catalog_const.getForeignkeycols()) {

                    // We store the name of the child column in the name of the ColumnRef catalog
                    // object to the parent
                    final Column catalog_child_col = cust_table.getColumns().get(catalog_parent_colref.getTypeName());
                    assertNotNull(catalog_child_col);
                    // Lame
                    boolean found_const_for_child = false;
                    for (final ConstraintRef catalog_constref : catalog_child_col.getConstraints()) {
                        if (catalog_constref.getConstraint().equals(catalog_const)) {
                            found_const_for_child = true;
                            break;
                        }
                    }
                    assertTrue(found_const_for_child);

                    final Column catalog_parent_col = catalog_parent_colref.getColumn();
                    assertEquals(dist_table, catalog_parent_col.getParent());
                }
                break;
            }
        }
        assertTrue(found);
    }
    
    public void testVerticalPartition() {
        String schemaPath = "";
        try {
            final URL url = new TPCCProjectBuilder().getDDLURL(true);
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        String targetTable = "CUSTOMER";
        String targetColumns[] = { "C_ID", "C_FIRST", "C_LAST" };
        String columns = "";
        for (String col : targetColumns)
            columns += String.format("<column>%s</column>", col);

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "<partitions><partition table='" + targetTable + "' column='" + targetColumns[0] + "' /></partitions>" +
            "<verticalpartitions><verticalpartition table='" + targetTable + "'>" + columns + "</verticalpartition></verticalpartitions>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final Catalog catalog = compiler.compileCatalog(projectPath, cluster_config);
        assertNotNull(catalog);

        // Make sure that our target table has the columns that we specified
        final Database catalog_db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertNotNull(catalog_db);

        final Table catalog_tbl = catalog_db.getTables().get(targetTable);
        assertNotNull(catalog_tbl);
        
        boolean found = false;
        for (MaterializedViewInfo catalog_view : catalog_tbl.getViews()) {
            assertFalse(found);
            if (catalog_view.getVerticalpartition() == false) continue;
            found = true;
            
            Collection<Column> catalog_cols = CatalogUtil.getColumns(catalog_view.getGroupbycols());
            assertNotNull(catalog_cols);
            assertEquals(targetColumns.length, catalog_cols.size());
            for (String columnName : targetColumns) {
                Column catalog_col = catalog_tbl.getColumns().get(columnName);
                assertNotNull(catalog_col);
                assert(catalog_cols.contains(catalog_col)) : "Missing " + catalog_col.fullName();
            } // FOR (cols)
            
            // Make sure the sys table for this vertical partition has an index
            Table view_tbl = catalog_view.getDest();
            assertNotNull(view_tbl);
            assert(view_tbl.getSystable());
            assertEquals(1, view_tbl.getIndexes().size());
        } // FOR (views)
        assertTrue(found);
    }
    
    public void testInvalidVerticalPartition() {
        String schemaPath = "";
        try {
            final URL url = new TPCCProjectBuilder().getDDLURL(true);
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        String targetTable = "CUSTOMER";
        String targetColumns[] = { "C_ID", "C_FIRST", "C_LAST", "C_FIRST" };
        String columns = "";
        for (String col : targetColumns)
            columns += String.format("<column>%s</column>", col);

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "<verticalpartitions><verticalpartition table=\"" + targetTable + "\">" + columns + "</verticalpartition></verticalpartitions>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final Catalog catalog = compiler.compileCatalog(projectPath, cluster_config);
        assert(catalog == null);
    }
    
    public void testUniqueProcedureStatementIds() {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile("create table T(ID INTEGER);");
        int expected = 5;
        String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaFile.getAbsolutePath() + "'/>" +
            "</schemas>" +
            "<procedures>";
        for (int i = 0; i < expected; i++) {
            simpleXML += "<procedure class='proc" + i + "'><sql>select * from T</sql></procedure>";
        }
        simpleXML += "</procedures>" +
                     "</database>" +
                     "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final Catalog catalog = compiler.compileCatalog(projectPath, cluster_config);
        assertNotNull(catalog);
        
        Set<Integer> proc_ids = new HashSet<Integer>();
        Set<Integer> stmt_ids = new HashSet<Integer>();
        
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        int actual_procs = 0;
        int actual_stmts = 0;
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            assertFalse(proc_ids.contains(catalog_proc.getId()));
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                assertFalse(stmt_ids.contains(catalog_stmt.getId()));
                stmt_ids.add(catalog_stmt.getId());
                if (catalog_proc.getSystemproc() == false) actual_stmts++;
            } // FOR
            proc_ids.add(catalog_proc.getId());
            if (catalog_proc.getSystemproc() == false) actual_procs++;
        } // FOR
        assertEquals(expected, actual_procs);
        assertEquals(expected, actual_stmts);
    }
}
