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

package org.apache.sqoop.util.rules;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class MiniKdcRule implements TestRule, MiniKdcProvider {

  private MiniKdc miniKdc;

  private Properties configuration;

  private File workDir;

  public MiniKdcRule() {
    this(MiniKdc.createConf());
  }

  public MiniKdcRule(Properties configuration) {
    File baseDir = Files.createTempDir();
    this.workDir = new File(baseDir, "MiniKdcWorkDir");
    this.configuration = configuration;
  }

  @Override
  public void start() {
    try {
      miniKdc = new MiniKdc(configuration, workDir);
      miniKdc.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      miniKdc.stop();
      FileUtils.deleteDirectory(workDir);
      configuration = null;
      miniKdc = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MiniKdc getMiniKdc() {
    return miniKdc;
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        base.evaluate();
        stop();
      }
    };
  }
}
