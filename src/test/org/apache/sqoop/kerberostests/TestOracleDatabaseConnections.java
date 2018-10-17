package org.apache.sqoop.kerberostests;

import static org.junit.Assert.assertFalse;

import com.sun.security.auth.module.Krb5LoginModule;
import oracle.jdbc.internal.OracleConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.sqoop.authentication.KerberosAuthenticator;
import org.apache.sqoop.db.DriverManagerJdbcConnectionFactory;
import org.apache.sqoop.db.decorator.KerberizedConnectionFactoryDecorator;
import org.apache.sqoop.manager.JdbcDrivers;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class TestOracleDatabaseConnections {

  public static final String ORACLE_CONNECTION_STRING = "jdbc:oracle:thin:@myhost:1521:ORA11G";

  @Test
  public void testNonKerberizedConnection() throws Exception {
    Class.forName(JdbcDrivers.ORACLE.getDriverClass());
    try (Connection connection = DriverManager.getConnection(ORACLE_CONNECTION_STRING, "SYSTEM", "Sqoop12345")) {
      assertFalse(connection.isClosed());
    }
  }

  @Test(expected = ClassCastException.class)
  public void testKerberosAuthenticator() throws Exception {
    KerberosAuthenticator marcoAuthenticator = new KerberosAuthenticator(createKerberosConfiguration(), "marco", "/Users/szabolcsvasas/marco.keytab");

    Properties prop = new Properties();
    prop.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES, "( KERBEROS5 )");

    DriverManagerJdbcConnectionFactory decorated = new DriverManagerJdbcConnectionFactory("oracle.jdbc.driver.OracleDriver", ORACLE_CONNECTION_STRING, null, null, prop);
    KerberizedConnectionFactoryDecorator kerberizedConnectionFactory = new KerberizedConnectionFactoryDecorator(decorated, marcoAuthenticator);

    kerberizedConnectionFactory.createConnection();
  }

  private Configuration createKerberosConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    // Adding a rule for the realm used by the MiniKdc since the default kerberos configuration might contain another realm.
    //configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTH_TO_LOCAL, buildKerberosRule());
    return configuration;
  }

  @Test
  public void testKerberizedConnection() throws Exception {
    // request user and password
    String username = "marco";
    final char[] password = "cloudera".toCharArray();
    // set the krb5.conf
    String krb5conf = "/etc/krb5.conf";
    System.out.println("krb5.conf exists(): " + new File(krb5conf).exists());
    System.setProperty("java.security.krb5.conf", krb5conf);
    // prepare a login
    Krb5LoginModule krb5Module = new Krb5LoginModule();
    Subject subject = new Subject();
    HashMap state = new HashMap();
    HashMap options = new HashMap();
    options.put("doNotPrompt", "false");
    options.put("useTicketCache", "false");
    options.put("principal", username);
    krb5Module.initialize(subject,
        new CallbackHandler() {
          public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
              if (callbacks[i] instanceof PasswordCallback) {
                PasswordCallback pc = (PasswordCallback) callbacks[i];
                pc.setPassword(password);
              } else {
                throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback!");
              }
            }
          }
        },
        state, options);
    boolean login = krb5Module.login();
    krb5Module.commit();
    if (!login) {
      throw new Exception("Cannot login using kerberos!");
    }
    System.out.println("Logged as user: " + subject.getPrincipals());
    // use the login for the thin driver
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = Subject.doAs(subject,
          new PrivilegedExceptionAction<Connection>() {

            @Override
            public Connection run() {
              Properties prop = new Properties();
              prop.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES, "( KERBEROS5 )");
              try {
                Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
                return DriverManager.getConnection(ORACLE_CONNECTION_STRING, prop);
              } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
                return null;
              }
            }
          }
      );
      stmt = conn.prepareStatement("select user from dual");
      rs = stmt.executeQuery();
      if (rs.next()) {
        System.out.println(rs.getString(1));
      }
    } finally {
      if (rs != null) try {
        rs.close();
      } catch (SQLException e) {
      }
      if (stmt != null) try {
        stmt.close();
      } catch (SQLException e) {
      }
      if (conn != null) try {
        conn.close();
      } catch (SQLException e) {
      }
    }
  }

}



