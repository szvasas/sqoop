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

package org.apache.sqoop.mapreduce.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.mapreduce.parquet.hadoop.HadoopParquetJobConfiguratorFactory;
import org.apache.sqoop.mapreduce.parquet.kite.KiteParquetJobConfiguratorFactory;

public final class ParquetJobConfiguratorFactoryProvider {

  public static final String PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY = "sqoop.parquet.job.implementation";

  public static final String PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP = "hadoop";

  public static final String PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KITE = "kite";

  private ParquetJobConfiguratorFactoryProvider() {
    throw new AssertionError("This class is meant for static use only.");
  }

  // TODO [szvasas]: add logging
  public static ParquetJobConfiguratorFactory createParquetJobConfiguratorFactory(Configuration configuration) {
    String implementation = configuration.get(PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY);
    if (PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP.equalsIgnoreCase(implementation)) {
      return new HadoopParquetJobConfiguratorFactory();
    } else if (PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KITE.equalsIgnoreCase(implementation)) {
      return new KiteParquetJobConfiguratorFactory();
    } else {
      throw new RuntimeException("Parquet job configurator implementation is set. Please make sure you set " + PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY + ".");
    }
  }

}
