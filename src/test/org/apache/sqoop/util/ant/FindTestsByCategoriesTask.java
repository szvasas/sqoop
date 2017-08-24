package org.apache.sqoop.util.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;


/*
Desired output:
"org/apache/sqoop/tool/TestBaseSqoopTool.java,org/apache/sqoop/orm/TestCompilationManager.java,org/apache/sqoop/TestSqoopOptions.java"
 */

public class FindTestsByCategoriesTask extends Task {

  private static final String TEST_PATTERN_PROPERTY = "test.pattern";

  private static final String TEST_CATEGORY_SEPARATOR = ",";

  private Collection<Class<?>> testCategories;

  public FindTestsByCategoriesTask() {
    testCategories = new ArrayList<>();
  }

  @Override
  public void execute() throws BuildException {

    System.out.println("Found these classes: " + testCategories);

  }

  public void setTestCategories(String testCategoriesString) {
    try {
      for (String testCategoryString : splitTestCategoriesString(testCategoriesString)) {
        testCategories.add(Class.forName(testCategoryString));
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private Collection<String> splitTestCategoriesString(String testCategoriesString) {
    return Arrays.asList(testCategoriesString.split(TEST_CATEGORY_SEPARATOR));
  }

}
