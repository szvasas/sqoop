package org.apache.sqoop.util;

import org.junit.runner.RunWith;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.TestWithParameters;

import java.lang.annotation.Annotation;

public class BlockJUnit4ClassRunnerWithParameters extends org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters {
  public BlockJUnit4ClassRunnerWithParameters(TestWithParameters test) throws InitializationError {
    super(test);
  }

  @Override
  protected Annotation[] getRunnerAnnotations() {
    Annotation[] allAnnotations = getTestClass().getAnnotations();
    Annotation[] annotationsWithoutRunWith = new Annotation[allAnnotations.length - 1];
    int i = 0;
    for (Annotation annotation: allAnnotations) {
      if (!annotation.annotationType().equals(RunWith.class)) {
        annotationsWithoutRunWith[i] = annotation;
        ++i;
      }
    }
    return annotationsWithoutRunWith;
  }
}
