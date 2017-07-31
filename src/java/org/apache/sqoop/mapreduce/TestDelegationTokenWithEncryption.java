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
package org.apache.sqoop.mapreduce;

import com.cloudera.sqoop.hbase.HBaseSecureCluster;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class TestDelegationTokenWithEncryption {

  @BeforeClass
  public static void setUp() throws Exception {
    // enable rpc encryption
    HBaseSecureCluster.setUp();
    try (Connection conn = ConnectionFactory.createConnection(HBaseSecureCluster.TEST_UTIL.getConfiguration())) {
      Token<? extends TokenIdentifier> token = TokenUtil.obtainToken(conn);
      UserGroupInformation.getCurrentUser().addToken(token);
    }
  }

  private TableName getTestTableName() {
    return TableName.valueOf("hellotable");
  }

  @Test
  public void testPutGetWithDelegationToken() throws Exception {
    TableName tableName = getTestTableName();
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes("row");
    byte[] value = Bytes.toBytes("data");
    try (Connection conn = ConnectionFactory.createConnection(HBaseSecureCluster.TEST_UTIL.getConfiguration())) {
      Admin admin = conn.getAdmin();
      HTableDescriptor tableDescriptor = new HTableDescriptor(new HTableDescriptor(tableName));
      tableDescriptor.addFamily(new HColumnDescriptor(family));
      admin.createTable(tableDescriptor);
      try (Table table = conn.getTable(tableName)) {
        table.put(new Put(row).addColumn(family, qualifier, value));
        Result result = table.get(new Get(row));
        assertArrayEquals(value, result.getValue(family, qualifier));
      }
    }
  }
}
