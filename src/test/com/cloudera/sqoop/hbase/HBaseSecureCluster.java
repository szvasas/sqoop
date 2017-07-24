package com.cloudera.sqoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.token.TestGenerateDelegationToken;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

public class HBaseSecureCluster {

  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static String USERNAME;

  private static LocalHBaseCluster CLUSTER;

  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri()
      .getPath());
  private static MiniKdc KDC;

  private static String HOST = "localhost";

  private static String PRINCIPAL;

  private static String HTTP_PRINCIPAL;

  /**
   * Setup the security configuration for hdfs.
   */
  private static void setHdfsSecuredConfiguration(Configuration conf) throws Exception {
    // change XXX_USER_NAME_KEY to XXX_KERBEROS_PRINCIPAL_KEY after we drop support for hadoop-2.4.1
    conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, PRINCIPAL + "@" + KDC.getRealm());
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, KEYTAB_FILE.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY, PRINCIPAL + "@" + KDC.getRealm());
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, KEYTAB_FILE.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, HTTP_PRINCIPAL + "@"
        + KDC.getRealm());
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File keystoresDir = new File(TEST_UTIL.getDataTestDir("keystore").toUri().getPath());
    keystoresDir.mkdirs();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestGenerateDelegationToken.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(), sslConfDir, conf, false);

    conf.setBoolean("ignore.secure.ports.for.testing", true);
  }

  /**
   * Setup and start kerberos, hbase
   */
  @BeforeClass
  public static void setUp() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    USERNAME = UserGroupInformation.getLoginUser().getShortUserName();
    PRINCIPAL = USERNAME + "/" + HOST;
    HTTP_PRINCIPAL = "HTTP/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, HTTP_PRINCIPAL);
    TEST_UTIL.startMiniZKCluster();

    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSecuredConfiguration(TEST_UTIL.getConfiguration());

    //setHdfsSecuredConfiguration(TEST_UTIL.getConfiguration());
    UserGroupInformation.setConfiguration(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TokenProvider.class.getName());
    //TEST_UTIL.startMiniDFSCluster(1);
    Path rootdir = TEST_UTIL.getDataTestDirOnTestFS("TestGenerateDelegationToken");
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootdir);
    CLUSTER = new LocalHBaseCluster(TEST_UTIL.getConfiguration(), 1);
    CLUSTER.startup();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
    CLUSTER.join();
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.shutdownMiniCluster();
  }
}
