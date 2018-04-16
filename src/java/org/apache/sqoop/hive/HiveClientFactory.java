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

package org.apache.sqoop.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.authentication.KerberosAuthenticator;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.db.decorator.KerberizedConnectionFactoryDecorator;
import org.apache.sqoop.manager.ConnManager;

import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class HiveClientFactory {

  public HiveClient createHiveClient(SqoopOptions sqoopOptions, ConnManager connManager) {
    if (useHiveCli(sqoopOptions)) {
      return createHiveImport(sqoopOptions, connManager);
    } else {
      return createHiveServer2Client(sqoopOptions, connManager);
    }
  }

  private HiveClient createHiveImport(SqoopOptions sqoopOptions, ConnManager connManager) {
    return new HiveImport(sqoopOptions, connManager, sqoopOptions.getConf(), false);
  }

  private HiveClient createHiveServer2Client(SqoopOptions sqoopOptions, ConnManager connManager) {
    TableDefWriter tableDefWriter = createTableDefWriter(sqoopOptions, connManager);
    JdbcConnectionFactory hs2JdbcConnectionFactory = createJdbcConnectionFactory(sqoopOptions);
    return new HiveServer2Client(sqoopOptions, tableDefWriter, hs2JdbcConnectionFactory);
  }

  JdbcConnectionFactory createJdbcConnectionFactory(SqoopOptions sqoopOptions) {
    String connectionUsername = determineConnectionUsername(sqoopOptions);
    JdbcConnectionFactory connectionFactory = new HiveServer2ConnectionFactory(sqoopOptions.getHs2Url(), connectionUsername);
    if (useKerberizedConnection(sqoopOptions)) {
      KerberosAuthenticator authenticator = createKerberosAuthenticator(sqoopOptions);
      connectionFactory = new KerberizedConnectionFactoryDecorator(connectionFactory, authenticator);
    }
    return connectionFactory;
  }

  private String determineConnectionUsername(SqoopOptions sqoopOptions) {
    if (!isEmpty(sqoopOptions.getHs2User())) {
      return sqoopOptions.getHs2User();
    }
    try {
      return UserGroupInformation.getLoginUser().getUserName();
    } catch (IOException e) {
      throw new RuntimeException("Unable to determine login user.", e);
    }
  }

  KerberosAuthenticator createKerberosAuthenticator(SqoopOptions sqoopOptions) {
    return new KerberosAuthenticator(sqoopOptions.getConf(), sqoopOptions.getHs2User(), sqoopOptions.getHs2Keytab());
  }

  TableDefWriter createTableDefWriter(SqoopOptions sqoopOptions, ConnManager connManager) {
    return new TableDefWriter(sqoopOptions, connManager, sqoopOptions.getTableName(), sqoopOptions.getHiveTableName(), sqoopOptions.getConf(), false);
  }

  private boolean useHiveCli(SqoopOptions sqoopOptions) {
    return StringUtils.isEmpty(sqoopOptions.getHs2Url());
  }

  private boolean useKerberizedConnection(SqoopOptions sqoopOptions) {
    return !isBlank(sqoopOptions.getHs2Keytab());
  }

}
