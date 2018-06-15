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

package org.apache.sqoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.util.ParquetReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static org.apache.sqoop.Sqoop.SQOOP_RETHROW_PROPERTY;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY;
import static org.junit.Assert.assertEquals;

public class TestParquetIncrementalImportMerge extends ImportJobTestCase {

  private static final String[] TEST_COLUMN_TYPES = {"INTEGER", "VARCHAR(32)", "CHAR(64)", "TIMESTAMP"};

  private static final String[] ALTERNATIVE_TEST_COLUMN_TYPES = {"INTEGER", "INTEGER", "INTEGER", "TIMESTAMP"};

  private static final String INITIAL_RECORDS_TIMESTAMP = "2018-06-14 15:00:00.000";

  private static final String NEW_RECORDS_TIMESTAMP = "2018-06-14 16:00:00.000";

  private static final List<List<?>> INITIAL_RECORDS = asList(
      asList(2006, "Germany", "Italy", INITIAL_RECORDS_TIMESTAMP),
      asList(2014, "Brazil", "Hungary", INITIAL_RECORDS_TIMESTAMP)
  );

  private static final List<?> ALTERNATIVE_INITIAL_RECORD = asList(1, 2, 3, INITIAL_RECORDS_TIMESTAMP);

  private static final List<List<?>> NEW_RECORDS = asList(
      asList(2010, "South Africa", "Spain", NEW_RECORDS_TIMESTAMP),
      asList(2014, "Brazil", "Germany", NEW_RECORDS_TIMESTAMP)
  );

  private static final List<String> EXPECTED_MERGED_RECORDS = asList(
      "2006,Germany,Italy," + timeFromString(INITIAL_RECORDS_TIMESTAMP),
      "2010,South Africa,Spain," + timeFromString(NEW_RECORDS_TIMESTAMP),
      "2014,Brazil,Germany," + timeFromString(NEW_RECORDS_TIMESTAMP)
  );

  private static final List<String> EXPECTED_INITIAL_RECORDS = asList(
      "2006,Germany,Italy," + timeFromString(INITIAL_RECORDS_TIMESTAMP),
      "2014,Brazil,Hungary," + timeFromString(INITIAL_RECORDS_TIMESTAMP)
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Override
  public void setUp() {
    super.setUp();

    createTableWithColTypes(TEST_COLUMN_TYPES, toStringArray(INITIAL_RECORDS.get(0)));
    for (int i = 1; i < INITIAL_RECORDS.size(); i++) {
      insertIntoTable(TEST_COLUMN_TYPES, toStringArray(INITIAL_RECORDS.get(i)));
    }
  }

  @Test
  public void testVanillaMerge() throws Exception {
    String[] args = importArgs(getConnectString(), getTableName(), getTablePath().toString()).build();
    runImport(args);

    clearTable(getTableName());

    for (List<?> record : NEW_RECORDS) {
      insertIntoTable(TEST_COLUMN_TYPES, toStringArray(record));
    }

    args = incrementalImportArgs(getConnectString(), getTableName(), getTablePath().toString(), getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();

    runImport(args);

    List<String> result = new ParquetReader(getTablePath()).readAllInCsv();
    sort(result);

    assertEquals(EXPECTED_MERGED_RECORDS, result);
  }

  @Test
  public void testMergeWhenTheIncrementalImportDoesNotImportAnyRows() throws Exception {
    String[] args = importArgs(getConnectString(), getTableName(), getTablePath().toString()).build();
    runImport(args);

    clearTable(getTableName());

    args = incrementalImportArgs(getConnectString(), getTableName(), getTablePath().toString(), getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();

    runImport(args);

    List<String> result = new ParquetReader(getTablePath()).readAllInCsv();
    sort(result);

    assertEquals(EXPECTED_INITIAL_RECORDS, result);
  }

  @Test
  public void testMergeWithIncompatibleSchemas() throws Exception {
    System.setProperty(SQOOP_RETHROW_PROPERTY, "true");
    String targetDir = getWarehouseDir() + "/testMergeWithIncompatibleSchemas";
    String[] args = importArgs(getConnectString(), getTableName(), targetDir).build();
    runImport(args);

    incrementTableNum();
    createTableWithColTypes(ALTERNATIVE_TEST_COLUMN_TYPES, toStringArray(ALTERNATIVE_INITIAL_RECORD));

    args = incrementalImportArgs(getConnectString(), getTableName(), targetDir, getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();

    SqoopOptions opts = getSqoopOptions(getConf());
    Sqoop sqoop = new Sqoop(new ImportTool(), getConf(), opts);

    expectedException.expectMessage("Cannot merge files, the Avro schemas are not compatible.");
    Sqoop.runSqoop(sqoop, args);
  }

  private static ArgumentArrayBuilder importArgs(String connectString, String tableName, String targetDir) {
    return new ArgumentArrayBuilder()
        .withProperty(PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY, PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP)
        .withOption("connect", connectString)
        .withOption("table", tableName)
        .withOption("num-mappers", "1")
        .withOption("target-dir", targetDir)
        .withOption("as-parquetfile")
        .withOption("throw-on-error");
  }

  private static ArgumentArrayBuilder incrementalImportArgs(String connectString, String tableName, String targetDir, String checkColumn, String mergeKey, String lastValue) {
    return importArgs(connectString, tableName, targetDir)
        .withOption("incremental", "lastmodified")
        .withOption("check-column", checkColumn)
        .withOption("merge-key", mergeKey)
        .withOption("last-value", lastValue);
  }

  private static long timeFromString(String timeStampString) {
    try {
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      return format.parse(timeStampString).getTime();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private static String[] toStringArray(List<?> columnValues) {
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

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions sqoopOptions = super.getSqoopOptions(conf);
    sqoopOptions.setThrowOnError(true);
    return sqoopOptions;
  }
}
