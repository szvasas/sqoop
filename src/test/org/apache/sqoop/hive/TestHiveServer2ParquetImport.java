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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class TestHiveServer2ParquetImport extends ImportJobTestCase {

  @Parameters(name = "compressionCodec = {0}")
  public static Iterable<? extends Object> authenticationParameters() {
    return Arrays.asList("snappy", "gzip");
  }

  private static final String[] TEST_COLUMN_TYPES = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};

  private static final List<Object> TEST_COLUMN_VALUES = Arrays.<Object>asList("test", 42, "somestring");

  private static final List<Object> TEST_COLUMN_VALUES_LINE2 = Arrays.<Object>asList("test2", 4242, "somestring2");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final String compressionCodec;

  private static HiveMiniCluster hiveMiniCluster;

  private static HiveServer2TestUtil hiveServer2TestUtil;

  public TestHiveServer2ParquetImport(String compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  @BeforeClass
  public static void beforeClass() {
    hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
    hiveMiniCluster.start();
    hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
  }

  @AfterClass
  public static void afterClass() {
    hiveMiniCluster.stop();
  }

  @Override
  @Before
  public void setUp() {
    super.setUp();

    createTableWithColTypes(TEST_COLUMN_TYPES, toStringArray(TEST_COLUMN_VALUES));
  }

  @Override
  @After
  public void tearDown() {
    try {
      dropTableIfExists(getTableName());
    } catch (SQLException e) {
      throw new RuntimeException();
    }

    super.tearDown();
  }

  @Test
  public void testNormalHiveImportAsParquet() throws Exception {
    String[] args = commonArgs().build();

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertThat(rows, hasItems(TEST_COLUMN_VALUES));
  }

  @Test
  public void testAppendHiveImportAsParquet() throws Exception {
    String[] args = commonArgs().build();

    runImport(args);

    insertIntoTable(TEST_COLUMN_TYPES, toStringArray(TEST_COLUMN_VALUES_LINE2));

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertThat(rows, hasItems(TEST_COLUMN_VALUES, TEST_COLUMN_VALUES_LINE2));
  }

  @Test
  public void testCreateOverwriteHiveImportAsParquet() throws Exception {
    String[] args = commonArgs()
        .withOption("hive-overwrite")
        .build();

    runImport(args);

    dropTableIfExists(getTableName());
    createTableWithColTypes(TEST_COLUMN_TYPES, toStringArray(TEST_COLUMN_VALUES_LINE2));

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertEquals(asList(TEST_COLUMN_VALUES_LINE2), rows);
  }

  @Test
  // TODO: this gives a bit different error message than the Kite one, it should be fixed.
  public void testCreateHiveImportAsParquet() throws Exception {
    String[] args = commonArgs()
        .withOption("create-hive-table")
        .build();

    runImport(args);

    expectedException.expect(IOException.class);
    runImport(args);
  }

  @Test
  public void testHiveImportAsParquetWhenTableExistsWithIncompatibleSchema() throws Exception {
    String hiveTableName = "hiveImportAsParquetWhenTableExistsWithIncompatibleSchema";
    String[] incompatibleSchemaTableTypes = {"INTEGER", "INTEGER", "INTEGER"};
    List<Object> incompatibleSchemaTableData = Arrays.<Object>asList(100, 200, 300);

    String[] args = commonArgs()
        .withOption("hive-table", hiveTableName)
        .build();

    runImport(args);

    // We make sure we create a new table in the test RDBMS.
    incrementTableNum();
    createTableWithColTypes(incompatibleSchemaTableTypes, toStringArray(incompatibleSchemaTableData));

    // Recreate the argument array to pick up the new RDBMS table name.
    args = commonArgs()
        .withOption("hive-table", hiveTableName)
        .build();

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
        .withOption("delete-target-dir")
        .withOption("compression-codec", compressionCodec);
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
