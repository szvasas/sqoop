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

package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;
import org.apache.sqoop.util.FileSystemUtil;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;

import java.io.IOException;

/**
 * An implementation of {@link ParquetExportJobConfigurator} which depends on the Kite Dataset API.
 */
public class KiteParquetExportJobConfigurator implements ParquetExportJobConfigurator {

  @Override
  public void configureInputFormat(Job job, Path inputPath) throws IOException {
    String uri = "dataset:" + FileSystemUtil.makeQualified(inputPath, job.getConfiguration());
    DatasetKeyInputFormat.configure(job).readFrom(uri);
  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return KiteParquetExportMapper.class;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DatasetKeyInputFormat.class;
  }
}
