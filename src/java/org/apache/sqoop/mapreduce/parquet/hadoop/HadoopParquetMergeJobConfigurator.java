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

package org.apache.sqoop.mapreduce.parquet.hadoop;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.mapreduce.MergeParquetMapper;
import org.apache.sqoop.mapreduce.parquet.ParquetMergeJobConfigurator;
import parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

import static org.apache.sqoop.mapreduce.parquet.ParquetConstants.SQOOP_PARQUET_AVRO_SCHEMA_KEY;

public class HadoopParquetMergeJobConfigurator implements ParquetMergeJobConfigurator {

  public static final Log LOG = LogFactory.getLog(HadoopParquetMergeJobConfigurator.class.getName());

  private final HadoopParquetImportJobConfigurator importJobConfigurator;

  private final HadoopParquetExportJobConfigurator exportJobConfigurator;

  public HadoopParquetMergeJobConfigurator(HadoopParquetImportJobConfigurator importJobConfigurator, HadoopParquetExportJobConfigurator exportJobConfigurator) {
    this.importJobConfigurator = importJobConfigurator;
    this.exportJobConfigurator = exportJobConfigurator;
  }

  public HadoopParquetMergeJobConfigurator() {
    this(new HadoopParquetImportJobConfigurator(), new HadoopParquetExportJobConfigurator());
  }

  @Override
  public void configureParquetMergeJob(Configuration conf, Job job, Path oldPath, Path newPath,
                                       Path finalPath) throws IOException {
    try {
      LOG.info("Trying to merge parquet files");
      job.setOutputKeyClass(Void.class);
      job.setMapperClass(MergeParquetMapper.class);
      job.setReducerClass(HadoopMergeParquetReducer.class);
      job.setOutputValueClass(GenericRecord.class);

      Schema oldPathAvroSchema = AvroUtil.getAvroSchemaFromParquetFile(oldPath, conf);
      Schema newPathAvroSchema = AvroUtil.getAvroSchemaFromParquetFile(newPath, conf);

      // TODO: schema comparison?
      // TODO: compression codec?
      job.setInputFormatClass(exportJobConfigurator.getInputFormatClass());
      AvroParquetInputFormat.setAvroReadSchema(job, oldPathAvroSchema);

      conf.set(SQOOP_PARQUET_AVRO_SCHEMA_KEY, oldPathAvroSchema.toString());
      importJobConfigurator.configureAvroSchema(job, oldPathAvroSchema);
      job.setOutputFormatClass(importJobConfigurator.getOutputFormatClass());
    } catch (Exception cnfe) {
      throw new IOException(cnfe);
    }
  }

}
