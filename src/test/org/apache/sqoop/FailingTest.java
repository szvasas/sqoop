package org.apache.sqoop;

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class FailingTest {

  @Test
  public void fail() {
    throw new RuntimeException("Fail!!!");
  }

}
