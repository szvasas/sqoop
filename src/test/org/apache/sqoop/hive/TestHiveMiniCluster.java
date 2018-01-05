package org.apache.sqoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
import org.apache.sqoop.hive.minicluster.*;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestHiveMiniCluster {

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdcInfrastructure = new MiniKdcInfrastructureRule();

  private static final String TEST_USERNAME = "sqoop";

  private static final String TEST_PASSWORD = "secret";

  @Parameters(name = "config = {0}")
  public static Iterable<? extends Object> authenticationParameters() {
    return Arrays.asList(new NoAuthenticationConfiguration(),
                         new PasswordAuthenticationConfiguration(TEST_USERNAME, TEST_PASSWORD),
                         new KerberosAuthenticationConfiguration(miniKdcInfrastructure));
  }

  private static final String CREATE_TABLE_SQL = "CREATE TABLE TestTable (id int)";

  private static final String INSERT_SQL = "INSERT INTO TestTable VALUES (?)";

  private static final String SELECT_SQL = "SELECT * FROM TestTable";

  private static final int TEST_VALUE = 10;

  private final AuthenticationConfiguration authenticationConfiguration;

  private HiveMiniCluster hiveMiniCluster;

  private JdbcConnectionFactory connectionFactory;

  public TestHiveMiniCluster(AuthenticationConfiguration authenticationConfiguration) {
    this.authenticationConfiguration = authenticationConfiguration;
  }

  @Before
  public void before() throws SQLException {
    hiveMiniCluster = new HiveMiniCluster(authenticationConfiguration);
    hiveMiniCluster.start();

    connectionFactory = new HiveServer2ConnectionFactory(hiveMiniCluster.getUrl(), TEST_USERNAME, TEST_PASSWORD);
  }

  @Test
  public void testInsertedRowCanBeReadFromTable() throws Exception {
    createTestTable();
    insertRowIntoTestTable();

    assertEquals(TEST_VALUE, getDataFromTestTable());
  }

  private void insertRowIntoTestTable() throws SQLException {
    try (Connection conn = connectionFactory.createConnection(); PreparedStatement stmnt = conn.prepareStatement(INSERT_SQL)) {
      stmnt.setInt(1, TEST_VALUE);
      stmnt.executeUpdate();
    }
  }

  private int getDataFromTestTable() throws SQLException {
    try (Connection conn = connectionFactory.createConnection(); PreparedStatement stmnt = conn.prepareStatement(SELECT_SQL)) {
      ResultSet resultSet = stmnt.executeQuery();
      resultSet.next();
      return resultSet.getInt(1);
    }
  }

  private void createTestTable() throws SQLException {
    try (Connection conn = connectionFactory.createConnection(); PreparedStatement stmnt = conn.prepareStatement(CREATE_TABLE_SQL)) {
      stmnt.executeUpdate();
    }
  }

  @After
  public void after() {
    hiveMiniCluster.stop();
    UserGroupInformation.setConfiguration(new Configuration());
  }

}