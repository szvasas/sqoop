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
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.util.ParquetReader;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY;
import static org.junit.Assert.assertEquals;

public class TestParquetIncrementalImportMerge extends ImportJobTestCase {

  private static final String[] TEST_COLUMN_TYPES = {"INTEGER", "VARCHAR(32)", "CHAR(64)", "TIMESTAMP"};

  private static final String INITIAL_RECORDS_TIMESTAMP = "2018-06-14 15:00:00.000";

  private static final String NEW_RECORDS_TIMESTAMP = "2018-06-14 16:00:00.000";

  private static final List<List<?>> INITIAL_RECORDS = asList(
      asList(2014, "Brazil", "Hungary", INITIAL_RECORDS_TIMESTAMP),
      asList(2006, "Germany", "Italy", INITIAL_RECORDS_TIMESTAMP)
  );

  private static final List<List<?>> NEW_RECORDS = asList(
      asList(2014, "Brazil", "Germany", NEW_RECORDS_TIMESTAMP),
      asList(2010, "South Africa", "Spain", NEW_RECORDS_TIMESTAMP)
  );

  private static final List<String> EXPECTED_RECORDS = asList(
      "2006,Germany,Italy," + timeFromString(INITIAL_RECORDS_TIMESTAMP),
      "2010,South Africa,Spain," + timeFromString(NEW_RECORDS_TIMESTAMP),
      "2014,Brazil,Germany," + timeFromString(NEW_RECORDS_TIMESTAMP)
  );

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
    String[] args = importArgs(getConnectString(), getTableName(), getWarehouseDir()).build();
    runImport(args);

    clearTable(getTableName());

    for (List<?> record : NEW_RECORDS) {
      insertIntoTable(TEST_COLUMN_TYPES, toStringArray(record));
    }

    args = incrementalImportArgs(getConnectString(), getTableName(), getWarehouseDir(), getColName(3), getColName(0), INITIAL_RECORDS_TIMESTAMP).build();

    runImport(args);

    List<String> result = new ParquetReader(getTablePath()).readAllInCsv();
    sort(result);
    
    assertEquals(EXPECTED_RECORDS, result);
  }

  private static ArgumentArrayBuilder importArgs(String connectString, String tableName, String warehouseDir) {
    return new ArgumentArrayBuilder()
        .withProperty(PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY, PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP)
        .withOption("connect", connectString)
        .withOption("table", tableName)
        .withOption("num-mappers", "1")
        .withOption("warehouse-dir", warehouseDir)
        .withOption("as-parquetfile");
  }

  private static ArgumentArrayBuilder incrementalImportArgs(String connectString, String tableName, String warehouseDir, String checkColumn, String mergeKey, String lastValue) {
    return importArgs(connectString, tableName, warehouseDir)
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
}
