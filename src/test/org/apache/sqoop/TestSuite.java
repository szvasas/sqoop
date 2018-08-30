package org.apache.sqoop;

import org.apache.sqoop.hcat.HCatalogImportTest;
import org.apache.sqoop.hive.TestHiveServer2ParquetImport;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses( {TestHiveServer2ParquetImport.class, HCatalogImportTest.class})
public class TestSuite {
}
