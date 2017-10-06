package org.apache.sqoop;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;

public class TestSqoopArchitecture extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(TestSqoopArchitecture.class.getName());

  @Test
  public void test() throws IOException {
//    Sqoop.main(new String[]{"job", "-Dsqoop.metastore.client.enable.autoconnect=false", "--list"});
    Sqoop.main(new String[]{"job", "--meta-connect", "jdbc:mysql://localhost:3306/sqoop", "--meta-username", "sqoop", "--meta-password", "Sqoop12345", "--list"});

  }

}