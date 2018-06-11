/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestHiveServer2ParquetImport extends ImportJobTestCase {

  private HiveMiniCluster hiveMiniCluster;

  private HiveServer2TestUtil hiveServer2TestUtil;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
    hiveMiniCluster.start();
    hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    hiveMiniCluster.stop();
  }

  @Test
  public void testNormalHiveImportAsParquet() throws Exception {
    List<Object> columnValues = Arrays.<Object>asList("test", 42, "somestring");

    String[] types = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};
    createTableWithColTypes(types, toStringArray(columnValues));

    String[] args = commonArgs().build();

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertThat(rows, hasItems(columnValues));
  }

  @Test
  public void testAppendHiveImportAsParquet() throws Exception {
    List<Object> firstLine = Arrays.<Object>asList("test", 42, "somestring");
    List<Object> appendedLine = Arrays.<Object>asList("test2", 4242, "somestring2");

    String[] types = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};
    createTableWithColTypes(types, toStringArray(firstLine));

    String[] args = commonArgs().build();

    runImport(args);

    insertIntoTable(types, toStringArray(appendedLine));

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertThat(rows, hasItems(firstLine, appendedLine));
  }

  @Test
  public void testCreateOverwriteHiveImportAsParquet() throws Exception {
    List<Object> firstLine = Arrays.<Object>asList("test", 42, "somestring");
    List<Object> overwriteLine = Arrays.<Object>asList("test2", 4242, "somestring2");

    String[] types = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};
    createTableWithColTypes(types, toStringArray(firstLine));

    String[] args = commonArgs()
        .withOption("hive-overwrite")
        .build();

    runImport(args);

    dropTableIfExists(getTableName());
    createTableWithColTypes(types, toStringArray(overwriteLine));

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertEquals(asList(overwriteLine), rows);
  }

  @Test(expected = IOException.class)
  public void testCreateHiveImportAsParquet() throws Exception {
    List<Object> firstLine = Arrays.<Object>asList("test", 42, "somestring");

    String[] types = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};
    createTableWithColTypes(types, toStringArray(firstLine));

    String[] args = commonArgs()
        .withOption("create-hive-table")
        .build();

    runImport(args);
    runImport(args);
  }

  private ArgumentArrayBuilder commonArgs() {
    return new ArgumentArrayBuilder()
        .withProperty(PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY, "hadoop")
        .withOption("connect", getConnectString())
        .withOption("table", getTableName())
        .withOption("hive-import")
        .withOption("hs2-url", hiveMiniCluster.getUrl())
        .withOption("split-by", getColName(1))
        .withOption("as-parquetfile")
        .withOption("delete-target-dir");
  }

  private String[] toStringArray(List<Object> columnValues) {
    String[] result = new String[columnValues.size()];

    for (int i = 0; i < columnValues.size(); i++) {
      if (columnValues.get(i) instanceof String) {
        result[i] = StringUtils.wrap((String) columnValues.get(i), '\'');
      } else {
        result[i] = columnValues.get(i).toString();
      }
    }

    return result;
  }

}
