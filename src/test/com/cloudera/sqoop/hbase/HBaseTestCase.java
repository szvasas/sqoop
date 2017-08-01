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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.util.rules.MiniKdcProvider;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Utility methods that facilitate HBase import tests.
 */
public abstract class HBaseTestCase extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      HBaseTestCase.class.getName());

  private final MiniKdcProvider miniKdcProvider;

  public HBaseTestCase() {
    this(null);
  }

  public HBaseTestCase(MiniKdcProvider miniKdcProvider) {
    this.miniKdcProvider = miniKdcProvider;
  }

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
      String zookeeperPort = hbaseTestUtil.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT);
      args.add("-D");
      args.add("hbase.zookeeper.property.clientPort=" + zookeeperPort);

      if (isKeberized()) {
        String principalForTesting = HBaseKerberosUtils.getPrincipalForTesting();
        args.add("-D");
        args.add("hbase.security.authentication=kerberos");
        args.add("-D");
        args.add("hbase.master.kerberos.principal=" + principalForTesting);
        args.add("-D");
        args.add("hbase.regionserver.kerberos.principal=" + principalForTesting);
        args.add("-D");
        args.add("yarn.resourcemanager.principal=" + principalForTesting);
      }
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
  private HBaseTestingUtility hbaseTestUtil;
  private MiniHBaseCluster hbaseCluster;
  private File rootDir;

  @Override
  @Before
  public void setUp() {
    try {
      rootDir = new File(TEMP_BASE_DIR + "HBaseTestCase" + UUID.randomUUID());
      rootDir.mkdirs();
      hbaseTestUtil = new HBaseTestingUtility();
      setupKerberos();

      hbaseTestUtil.startMiniZKCluster();

      hbaseTestUtil.getConfiguration().set(HConstants.HBASE_DIR, rootDir.getAbsolutePath());
      hbaseCluster = new MiniHBaseCluster(hbaseTestUtil.getConfiguration(), 1);
      hbaseCluster.startMaster();
      super.setUp();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private void setupKerberos() {
    if (!isKeberized()){
      return;
    }

    try {
      MiniKdc miniKdc = miniKdcProvider.getMiniKdc();

      File keytabFile = new File(rootDir.getAbsolutePath(), "keytab");
      keytabFile.createNewFile();
      HBaseKerberosUtils.setKeytabFileForTesting(keytabFile.getAbsolutePath());
      String userName = UserGroupInformation.getLoginUser().getShortUserName();
      String principal = userName + "/localhost";
      miniKdc.createPrincipal(keytabFile, principal);

      HBaseKerberosUtils.setPrincipalForTesting(principal + "@" + miniKdc.getRealm());
      HBaseKerberosUtils.setSecuredConfiguration(hbaseTestUtil.getConfiguration());

      UserGroupInformation.setConfiguration(hbaseTestUtil.getConfiguration());
      hbaseTestUtil.getConfiguration().setStrings(REGION_COPROCESSOR_CONF_KEY, TokenProvider.class.getName());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() throws Exception {
    LOG.info("In shutdown() method");
    LOG.info("Shutting down HBase cluster");
    hbaseCluster.shutdown();
    hbaseCluster.join();
    hbaseTestUtil.shutdownMiniCluster();
    hbaseTestUtil = null;
    FileUtils.deleteDirectory(rootDir);
    LOG.info("shutdown() method returning.");
  }

  @Override
  @After
  public void tearDown() {
    try {
      shutdown();
    } catch (Exception e) {
      LOG.warn("Error shutting down HBase minicluster: "
              + StringUtils.stringifyException(e));
    }
    super.tearDown();
  }

  protected void verifyHBaseCell(String tableName, String rowKey,
      String colFamily, String colName, String val) throws IOException {
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
    HTable table = new HTable(new Configuration(
        hbaseTestUtil.getConfiguration()), Bytes.toBytes(tableName));
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

  protected int countHBaseTable(String tableName, String colFamily)
      throws IOException {
    int count = 0;
    HTable table = new HTable(new Configuration(
        hbaseTestUtil.getConfiguration()), Bytes.toBytes(tableName));
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

  protected boolean isKeberized() {
    return miniKdcProvider != null;
  }

}
