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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.mapreduce.parquet.hadoop.HadoopParquetJobConfiguratorFactory;
import org.apache.sqoop.mapreduce.parquet.kite.KiteParquetJobConfiguratorFactory;

import static java.lang.String.format;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorImplementation.HADOOP;
import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorImplementation.KITE;

public final class ParquetJobConfiguratorFactoryProvider {

  public static final String PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY = "sqoop.parquet.job.implementation";

  public static final String PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_HADOOP = "hadoop";

  public static final String PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KITE = "kite";

  private static final Log LOG = LogFactory.getLog(ParquetJobConfiguratorFactoryProvider.class.getName());

  private ParquetJobConfiguratorFactoryProvider() {
    throw new AssertionError("This class is meant for static use only.");
  }

  public static ParquetJobConfiguratorFactory createParquetJobConfiguratorFactory(ParquetJobConfiguratorImplementation implementation) {
    ParquetJobConfiguratorFactory jobConfiguratorFactory = null;

    if (implementation == HADOOP) {
      jobConfiguratorFactory = new HadoopParquetJobConfiguratorFactory();
    } else if (implementation == KITE) {
      jobConfiguratorFactory = new KiteParquetJobConfiguratorFactory();
    }

    LOG.info(format("Configured %s: %s", PARQUET_JOB_CONFIGURATOR_IMPLEMENTATION_KEY, implementation));
    LOG.debug(format("Using ParquetJobConfiguratorFactory class: %s", jobConfiguratorFactory.getClass().getName()));
    return jobConfiguratorFactory;
  }

}
