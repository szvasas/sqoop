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

package org.apache.sqoop.metastore;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.apache.sqoop.metastore.AutoGenericJobStorage.AUTO_STORAGE_CONNECT_STRING_KEY;
import static org.apache.sqoop.metastore.AutoGenericJobStorage.AUTO_STORAGE_IS_ACTIVE_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestAutoGenericJobStorageCanAccept {

  @Parameters(name = "autoStorageIsActive = {0}, autoStorageConnectString = {1}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(new Object[]{false, null, false},
                         new Object[]{false, "jdbc:mysql://localhost/", false},
                         new Object[]{false, "jdbc:dummy://localhost/", false},
                         new Object[]{true, null, true},
                         new Object[]{true, "jdbc:mysql://localhost/", true},
                         new Object[]{true, "jdbc:dummy://localhost/", false});
  }

  private final boolean autoStorageIsActive;

  private final String autoStorageConnectString;

  private final boolean canAccept;

  private final Map<String, String> descriptor;

  private AutoGenericJobStorage jobStorage;

  public TestAutoGenericJobStorageCanAccept(boolean autoStorageIsActive, String autoStorageConnectString, boolean canAccept) {
    this.autoStorageIsActive = autoStorageIsActive;
    this.autoStorageConnectString = autoStorageConnectString;
    this.canAccept = canAccept;
    this.descriptor = emptyMap();
  }

  @Before
  public void before() {
    jobStorage = new AutoGenericJobStorage();
    Configuration configuration = new Configuration();
    configuration.setBoolean(AUTO_STORAGE_IS_ACTIVE_KEY, autoStorageIsActive);

    if (autoStorageConnectString == null) {
      configuration.unset(AUTO_STORAGE_CONNECT_STRING_KEY);
    } else {
      configuration.set(AUTO_STORAGE_CONNECT_STRING_KEY, autoStorageConnectString);
    }

    jobStorage.setConf(configuration);
  }

  @Test
  public void testCanWithParameters() {
    assertEquals(canAccept, jobStorage.canAccept(descriptor));
  }

}