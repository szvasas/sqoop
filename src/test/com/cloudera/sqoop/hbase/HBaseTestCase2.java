/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sqoop.hbase;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Utility methods that facilitate HBase import tests.
 */
public abstract class HBaseTestCase2 extends ImportJobTestCase {

  /*
   * This is to restore test.build.data system property which gets reset
   * when HBase tests are run. Since other tests in Sqoop also depend upon
   * this property, they can fail if are run subsequently in the same VM.
   */
  private static String testBuildDataProperty = "";

  private static void recordTestBuildDataProperty() {
    testBuildDataProperty = System.getProperty("test.build.data", "");
  }

  private static void restoreTestBuidlDataProperty() {
    System.setProperty("test.build.data", testBuildDataProperty);
  }

  public static final Log LOG = LogFactory.getLog(
      HBaseTestCase2.class.getName());

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags,
      String hbaseTable, String hbaseColFam, boolean hbaseCreate,
      String queryStr) {

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
      args.add("-D");
      String zookeeperPort = HBaseSecureCluster.TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT);
      args.add("hbase.zookeeper.property.clientPort=" + zookeeperPort);
    }

    if (null != queryStr) {
      args.add("--query");
      args.add(queryStr);
    } else {
      args.add("--table");
      args.add(getTableName());
    }
    args.add("--split-by");
    args.add(getColName(0));
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--num-mappers");
    args.add("1");
    args.add("--column-family");
    args.add(hbaseColFam);
    args.add("--hbase-table");
    args.add(hbaseTable);
    if (hbaseCreate) {
      args.add("--hbase-create-table");
    }

    return args.toArray(new String[0]);
  }

  @Override
  @Before
  public void setUp() {
    try {
      HBaseSecureCluster.setUp();
      super.setUp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  @After
  public void tearDown() {
    try {
      HBaseSecureCluster.tearDown();
      super.tearDown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void verifyHBaseCell(String tableName, String rowKey,
      String colFamily, String colName, String val) throws IOException {
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
    HTable table = new HTable(new Configuration(
        HBaseSecureCluster.TEST_UTIL.getConfiguration()), Bytes.toBytes(tableName));
    try {
      Result r = table.get(get);
      byte [] actualVal = r.getValue(Bytes.toBytes(colFamily),
          Bytes.toBytes(colName));
      if (null == val) {
        assertNull("Got a result when expected null", actualVal);
      } else {
        assertNotNull("No result, but we expected one", actualVal);
        assertEquals(val, Bytes.toString(actualVal));
      }
    } finally {
      table.close();
    }
  }
  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File tempDir = new File(baseDir, UUID.randomUUID().toString());
    if (tempDir.mkdir()) {
      return tempDir;
    }
    throw new IllegalStateException("Failed to create directory");
  }

  protected int countHBaseTable(String tableName, String colFamily)
      throws IOException {
    int count = 0;
    HTable table = new HTable(new Configuration(
        HBaseSecureCluster.TEST_UTIL.getConfiguration()), Bytes.toBytes(tableName));
    try {
      ResultScanner scanner = table.getScanner(Bytes.toBytes(colFamily));
      for(Result result = scanner.next();
          result != null;
          result = scanner.next()) {
        count++;
      }
    } finally {
      table.close();
    }
    return count;
  }
}
