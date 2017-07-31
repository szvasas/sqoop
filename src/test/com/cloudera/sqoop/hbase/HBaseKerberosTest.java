package com.cloudera.sqoop.hbase;

import org.junit.Test;

import java.io.IOException;

public class HBaseKerberosTest extends HBaseTestCase {

  @Test
  public void testBasicUsage() throws IOException {
    // Create the HBase table in Sqoop as we run the job.
    String [] argv = getArgv(true, "BasicUsage", "BasicColFam", true, null);
    String [] types = { "INT", "INT" };
    String [] vals = { "0", "1" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("BasicUsage", "0", "BasicColFam", getColName(1), "1");
  }
}
