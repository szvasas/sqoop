package org.apache.sqoop.util.ant;

import org.apache.commons.lang3.StringUtils;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Path;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import static org.apache.commons.lang3.StringUtils.EMPTY;


public class FindTestsByCategoriesTask extends Task {

  private static final String TEST_PATTERN_PROPERTY = "test.pattern";

  private static final String SEPARATOR = ",";

  private static final String TEST_POSTFIX = ".java";

  private Collection<Class<?>> testCategories;

  private Path testDir;

  public FindTestsByCategoriesTask() {
    testCategories = new ArrayList<>();
  }

  @Override
  public void execute() throws BuildException {
    System.out.println("Searching for tests with the following categories: " + testCategories);
    Collection<Class<?>> testsByCategories = findTestsToRun();
    Collection<String> filesetElements = transformToFilesetFormat(testsByCategories);
    getProject().setNewProperty(TEST_PATTERN_PROPERTY, StringUtils.join(filesetElements, SEPARATOR));
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

  public void setTestDir(Path testDir) {
    this.testDir = testDir;
  }

  private Collection<String> splitTestCategoriesString(String testCategoriesString) {
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
    for (Class<?> categoryToInclude : testCategories) {
      if (categoryToInclude.isAssignableFrom(testCategory)) {
        return true;
      }
    }
    return false;
  }

  private Collection<Class<?>> findTestClasses() {
    try {
      Collection<Class<?>> result = new ArrayList<>();
      File root = new File(testDir.toString());
      Collection<String> testClassNames = findTestClassesRecursively(root, EMPTY);
      for (String testClassName : testClassNames) {
        Class<?> e = Class.forName(testClassName);
        if (e.isAnnotationPresent(Category.class)) {
          result.add(e);
        }
      }
      return result;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private Collection<String> findTestClassesRecursively(File root, String packageName) {
    Collection<String> result = new ArrayList<>();
    for (File file : root.listFiles(new ClassFilter())) {
      if (file.isFile()) {
        result.add(appendToPackage(packageName, stripClassExtension(file.getName())));
      } else if (file.isDirectory()) {
        result.addAll(findTestClassesRecursively(file, appendToPackage(packageName, file.getName())));
      }
    }
    return result;
  }

  private String stripClassExtension(String classFile) {
    if (!classFile.endsWith(".class")) {
      return classFile;
    }

    return classFile.substring(0, classFile.length() - ".class".length());
  }

  private String appendToPackage(String packageName, String fileName) {
    if (packageName.isEmpty()) {
      return fileName;
    }
    return packageName + "." + fileName;
  }

  private static class ClassFilter implements FileFilter {

    @Override
    public boolean accept(File pathname) {
      return pathname.isDirectory() || pathname.getName().endsWith(".class");
    }
  }
}
