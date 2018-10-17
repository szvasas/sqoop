package org.apache.sqoop.kerberostests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KerberosUtil {

  private static class SqoopKerberosConfiguration extends javax.security.auth.login.Configuration {

    private AppConfigurationEntry sqoopKerberosConfig() {
      Map<String, Object> options = new HashMap<>();
      options.put("useTicketCache", "false");
      options.put("useKeyTab", "true");
      options.put("doNotPrompt", "true");
      options.put("useFirstPass", "false");
      options.put("keyTab", "file:/Users/szabolcsvasas/marco.keytab");
      options.put("principal", "marco@EXAMPLE.COM");

      AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
      return appConfigurationEntry;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      AppConfigurationEntry sqoopKerberosConfig = sqoopKerberosConfig();
      return new AppConfigurationEntry[]{sqoopKerberosConfig};
    }
  }

  private static class SqoopKerberosPasswordConfiguration extends javax.security.auth.login.Configuration {

    private AppConfigurationEntry sqoopKerberosConfig() {
      Map<String, Object> options = new HashMap<>();
      options.put("useTicketCache", "false");
      options.put("useKeyTab", "false");
      options.put("doNotPrompt", "false");
      options.put("useFirstPass", "false");
      options.put("principal", "marco@EXAMPLE.COM");

      AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
      return appConfigurationEntry;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      AppConfigurationEntry sqoopKerberosConfig = sqoopKerberosConfig();
      return new AppConfigurationEntry[]{sqoopKerberosConfig};
    }
  }

  private static class SqoopKerberosTgtConfiguration extends javax.security.auth.login.Configuration {

    private final String ticketCache;

    private SqoopKerberosTgtConfiguration(String ticketCache) {
      this.ticketCache = ticketCache;
    }

    private AppConfigurationEntry sqoopKerberosConfig() {
      Map<String, Object> options = new HashMap<>();
      options.put("useTicketCache", "true");
      options.put("useKeyTab", "false");
      options.put("doNotPrompt", "true");
      options.put("useFirstPass", "false");
      options.put("ticketCache", ticketCache);
      options.put("principal", "marco@EXAMPLE.COM");

      AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
      return appConfigurationEntry;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      AppConfigurationEntry sqoopKerberosConfig = sqoopKerberosConfig();
      return new AppConfigurationEntry[]{sqoopKerberosConfig};
    }
  }

  private static class SqoopKerberosKeytabConfiguration extends javax.security.auth.login.Configuration {

    private final String keytab;

    private SqoopKerberosKeytabConfiguration(String keytab) {
      this.keytab = keytab;
    }

    private AppConfigurationEntry sqoopKerberosConfig() {
      Map<String, Object> options = new HashMap<>();
      options.put("useTicketCache", "false");
      options.put("useKeyTab", "true");
      options.put("doNotPrompt", "true");
      options.put("useFirstPass", "false");
      options.put("keyTab", keytab);
      options.put("principal", "marco@EXAMPLE.COM");

      AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
      return appConfigurationEntry;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      AppConfigurationEntry sqoopKerberosConfig = sqoopKerberosConfig();
      return new AppConfigurationEntry[]{sqoopKerberosConfig};
    }
  }

  private static final class PasswordProviderCallbackHandler implements CallbackHandler {

    private final char[] password;

    private PasswordProviderCallbackHandler(char[] password) {
      this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      if (callbacks.length == 0) {
        return;
      }
      if (! (callbacks[0] instanceof PasswordCallback)) {
        return;
      }
      PasswordCallback passwordCallback = (PasswordCallback) callbacks[0];
      passwordCallback.setPassword(password);
    }
  }

  public static UserGroupInformation loginUser() {
    try {
      System.setProperty("sun.security.krb5.principal", "sqoop");

      Configuration conf = new Configuration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "KERBEROS");

      UserGroupInformation.setConfiguration(conf);

      return UserGroupInformation.loginUserFromKeytabAndReturnUGI("sqoop", "/root/sqoop.keytab");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Subject loginSubject() {
    try {
      LoginContext loginContext = new LoginContext("SQLJDBCDriver", null, null, new SqoopKerberosConfiguration());
      loginContext.login();
      return loginContext.getSubject();
    } catch (LoginException e) {
      throw new RuntimeException(e);
    }
  }

  public static Subject loginSubjectWithPassword(char[] password) {
    try {
      // TODO: shall we copy the password array here? the logincontext will erase it during the authentication.
      PasswordProviderCallbackHandler passwordHandler = new PasswordProviderCallbackHandler(password);
      LoginContext loginContext = new LoginContext("SQLJDBCDriver", null, passwordHandler, new SqoopKerberosPasswordConfiguration());
      loginContext.login();
      return loginContext.getSubject();
    } catch (LoginException e) {
      throw new RuntimeException(e);
    }
  }

  public static Subject loginSubjectWithTgtCache(String tgtCache) {
    try {
      LoginContext loginContext = new LoginContext("SQLJDBCDriver", null, null, new SqoopKerberosTgtConfiguration(tgtCache));
      loginContext.login();
      return loginContext.getSubject();
    } catch (LoginException e) {
      throw new RuntimeException("Failed to connect with tgtCache: " + tgtCache, e);
    }
  }

  public static Subject loginSubjectWithKeyTab(String keyTab) {
    try {
      LoginContext loginContext = new LoginContext("SQLJDBCDriver", null, null, new SqoopKerberosKeytabConfiguration(keyTab));
      loginContext.login();
      return loginContext.getSubject();
    } catch (LoginException e) {
      throw new RuntimeException("Failed to connect with keytab: " + keyTab, e);
    }
  }

}
