package org.apache.sqoop.util.ant;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.*;

public class FindTestsByCategoriesTask extends Task {

  private static final String TEST_PATTERN_PROPERTY = "test.pattern";

  private static final String SEPARATOR = ",";

  private static final String TEST_POSTFIX = ".java";

  private Collection<Class<?>> includeCategories;

  private Collection<Class<?>> excludeCategories;

  private String testDir;

  public FindTestsByCategoriesTask() {
    includeCategories = new HashSet<>();
    excludeCategories = new HashSet<>();
  }

  @Override
  public void execute() throws BuildException {
    System.out.println("Including categories: " + includeCategories);
    System.out.println("Excluding categories: " + excludeCategories);
    Collection<Class<?>> testsByCategories = findTestsToRun();
    Collection<String> filesetElements = transformToFilesetFormat(testsByCategories);
    getProject().setNewProperty(TEST_PATTERN_PROPERTY, StringUtils.join(filesetElements, SEPARATOR));
  }

  public void setIncludeCategories(String includeCategoriesString) {
    includeCategories.addAll(parseCategoriesString(includeCategoriesString));
  }

  public void setExcludeCategories(String excludeCategoriesString) {
    excludeCategories.addAll(parseCategoriesString(excludeCategoriesString));
  }

  private Collection<Class<?>> parseCategoriesString(String categoriesString) {
    Collection<Class<?>> result = new HashSet<>();
    try {
      for (String testCategoryString : splitTestCategoriesString(categoriesString)) {
        result.add(Class.forName(testCategoryString));
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public void setTestDir(String testDir) {
    this.testDir = testDir;
  }

  private Collection<String> splitTestCategoriesString(String testCategoriesString) {
    if (testCategoriesString == null || testCategoriesString.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(testCategoriesString.split(SEPARATOR));
  }

  private Collection<String> transformToFilesetFormat(Collection<Class<?>> testClasses) {
    Collection<String> result = new ArrayList<>();

    for (Class<?> testClass : testClasses) {
      result.add(transformToFilesetFormat(testClass));
    }

    return result;
  }

  private String transformToFilesetFormat(Class<?> testClass) {
    String className = testClass.getCanonicalName();
    String pathToClass = className.replace('.', '/');
    return pathToClass + TEST_POSTFIX;
  }

  private Collection<Class<?>> findTestsToRun() {

    Collection<Class<?>> categorizedTests = findTestClasses();
    Collection<Class<?>> testsToRun = new HashSet<>();

    for (Class<?> categorizedTestClass : categorizedTests) {
      Category categoryAnnotation = categorizedTestClass.getAnnotation(Category.class);
      for (Class<?> testCategory : categoryAnnotation.value()) {
        if (isApplicable(testCategory)) {
          testsToRun.add(categorizedTestClass);
        }
      }
    }

    return testsToRun;
  }

  private boolean isApplicable(Class<?> testCategory) {
    for (Class<?> categoryToInclude : includeCategories) {
      if (categoryToInclude.isAssignableFrom(testCategory)) {
        return true;
      }
    }
    return false;
  }

  private Collection<Class<?>> findTestClasses() {
    try {
      Collection<Class<?>> result = new ArrayList<>();
      Collection<String> testClassNames = findTestClassesRecursively();
      for (String testClassName : testClassNames) {
        Class<?> e = Class.forName(testClassName.replace('/', '.'));
        if (e.isAnnotationPresent(Category.class)) {
          result.add(e);
        }
      }
      return result;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private Collection<String> findTestClassesRecursively() {
    Collection<File> classFiles = FileUtils.listFiles(new File(testDir), new String[]{"class"}, true);
    Collection<String> result = new ArrayList<>();
    for (File file : classFiles) {
      result.add(getClassNameFromClassAbsolutePath(testDir, file.getAbsolutePath()));
    }
    return result;
  }

  private static String getClassNameFromClassAbsolutePath(String root, String absolutePath) {
    return absolutePath.substring(root.length() + 1, absolutePath.length() - ".class".length());
  }
}
