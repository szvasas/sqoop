/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sqoop.metastore;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.JobTool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestMetastoreConfigurationParameters {

    private static final int STATUS_FAILURE = 1;
    private static final int STATUS_SUCCESS = 0;
    private static String TEST_USER = "sqoop";
    private static String TEST_PASSWORD = "sqoop";
    private static HsqldbTestServer testHsqldbServer;

    private Sqoop sqoop;

    @BeforeClass
    public static void beforeClass() throws Exception {
        testHsqldbServer = new HsqldbTestServer();
        testHsqldbServer.start();
        setupUsersForTesting();
    }

    @AfterClass
    public static void afterClass() {
        testHsqldbServer.stop();
    }

    @Before
    public void before() {
        sqoop = new Sqoop(new JobTool());
    }

    @Test
    public void testJobToolWithAutoConnectDisabledFails() throws IOException {
        String[] arguments = getArgsWithConfigs(asList("sqoop.metastore.client.enable.autoconnect=false"));
        assertEquals(STATUS_FAILURE, Sqoop.runSqoop(sqoop, arguments));
    }

    @Test
    public void testJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecifiedSuccessfullyRuns() {
        int status = runJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecified();
        assertEquals(STATUS_SUCCESS, status);
    }

    @Test
    public void testJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecifiedInitializesSpecifiedDatabase() throws SQLException {
        runJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecified();
        verifyMetastoreIsInitialized();
    }

    private int runJobToolWithAutoConnectUrlAndCorrectUsernamePasswordSpecified() {
        String url = "sqoop.metastore.client.autoconnect.url=" + HsqldbTestServer.getUrl();
        String user = "sqoop.metastore.client.autoconnect.username=" + TEST_USER;
        String password = "sqoop.metastore.client.autoconnect.password=" + TEST_PASSWORD;

        String[] arguments = getArgsWithConfigs(asList(url, user, password));
        return Sqoop.runSqoop(sqoop, arguments);
    }

    private String[] getArgsWithConfigs(List<String> configs) {
        List<String> args = new ArrayList<>();

        for (String config : configs) {
            args.add("-D");
            args.add(config);
        }

        args.add("--list");

        return args.toArray(new String[0]);
    }

    private static void setupUsersForTesting() throws SQLException {
        try (Connection connection = testHsqldbServer.getConnection(); Statement statement = connection.createStatement()) {
            // We create a new user and change the password of SA to make sure that Sqoop does not connect to metastore with the default user and password.
            statement.executeUpdate(String.format("CREATE USER %s PASSWORD %s ADMIN", TEST_USER, TEST_PASSWORD));
            statement.executeUpdate("ALTER USER \"SA\" SET PASSWORD \"NOT_DEFAULT\"");
        }
    }

    private void verifyMetastoreIsInitialized() throws SQLException {
        try (Connection connection = testHsqldbServer.getConnection(TEST_USER, TEST_PASSWORD); Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SQOOP_ROOT");
            assertTrue(resultSet.next());
        }
    }

}
