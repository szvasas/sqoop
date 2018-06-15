package org.apache.sqoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactoryProvider.PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY;

public class TestParquetMerge extends ImportJobTestCase {

  private static final String OLD_PATH = "/merge_old";

  private static final String NEW_PATH = "/merge_new";

  private static final String[] TEST_COLUMN_TYPES = {"INTEGER", "VARCHAR(32)", "CHAR(64)"};

  private static final List<List<?>> INITIAL_RECORDS = asList(
      asList(2014, "Brazil", "Hungary"),
      asList(2006, "Germany", "Italy")
  );

  private static final List<List<?>> NEW_RECORDS = asList(
      asList(2014, "Brazil", "Germany"),
      asList(2010, "South Africa", "Spain")
  );

  private static final List<List<?>> EXPECTED_RECORDS = asList(
      asList(2014, "Brazil", "Germany"),
      asList(2010, "South Africa", "Spain"),
      asList(2006, "Germany", "Italy")
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
    String[] args = importArgs(getConnectString(), getTableName(), getWarehouseDir() + OLD_PATH).build();
    runImport(args);

    clearTable(getTableName());

    for (List<?> record : NEW_RECORDS) {
      insertIntoTable(TEST_COLUMN_TYPES, toStringArray(record));
    }

    args = importArgs(getConnectString(), getTableName(), getWarehouseDir() + NEW_PATH).build();
    runImport(args);


  }

  private static ArgumentArrayBuilder mergeArgs(String connectString, String tableName, String targetDir) {
    return new ArgumentArrayBuilder()
        .withProperty(PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY, PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP)
        .withOption("connect", connectString)
        .withOption("table", tableName)
        .withOption("num-mappers", "1")
        .withOption("target-dir", targetDir)
        .withOption("as-parquetfile")
        .withOption("delete-target-dir");
  }
  
  private static ArgumentArrayBuilder importArgs(String connectString, String tableName, String targetDir) {
    return new ArgumentArrayBuilder()
        .withProperty(PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY, PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP)
        .withOption("connect", connectString)
        .withOption("table", tableName)
        .withOption("num-mappers", "1")
        .withOption("target-dir", targetDir)
        .withOption("as-parquetfile")
        .withOption("delete-target-dir");
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
