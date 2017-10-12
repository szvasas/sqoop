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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.sqoop.metastore.GenericJobStorage.META_CONNECT_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestGenericJobStorageCanAccept {

  @Parameterized.Parameters(name = "autoStorageIsActive = {0}, autoStorageConnectString = {1}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(new Object[]{null, false},
        new Object[]{"jdbc:mysql://localhost/", true},
        new Object[]{"jdbc:oracle://localhost/", true},
        new Object[]{"jdbc:hsqldb://localhost/", true},
        new Object[]{"jdbc:postgresql://localhost/", true},
        new Object[]{"jdbc:sqlserver://localhost/", true},
        new Object[]{"jdbc:db2://localhost/", true},
        new Object[]{"jdbc:dummy://localhost/", false});
  }

  private final String connectionString;

  private final boolean canAccept;

  private Map<String, String> descriptor;

  private GenericJobStorage jobStorage;

  public TestGenericJobStorageCanAccept(String connectionString, boolean canAccept) {
    this.connectionString = connectionString;
    this.canAccept = canAccept;
  }

  @Before
  public void before() {
    jobStorage = new GenericJobStorage();

    descriptor = new HashMap<>();
    descriptor.put(META_CONNECT_KEY, connectionString);
  }

  @Test
  public void testCanAcceptWithParameters() {
    assertEquals(canAccept, jobStorage.canAccept(descriptor));
  }

}