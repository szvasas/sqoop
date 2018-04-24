package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.mapreduce.ParquetImportMapper;
import org.apache.sqoop.mapreduce.ParquetJob;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;
import org.apache.sqoop.util.FileSystemUtil;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

import java.io.IOException;

public class KiteParquetImportJobConfigurator implements ParquetImportJobConfigurator {

  public static final Log LOG = LogFactory.getLog(KiteParquetImportJobConfigurator.class.getName());

  @Override
  public void configureMapper(JobConf conf, Schema schema, SqoopOptions options, String tableName, Path destination) throws IOException {
    String uri = getKiteUri(conf, options, tableName, destination);
    ParquetJob.WriteMode writeMode;

    if (options.doHiveImport()) {
      if (options.doOverwriteHiveTable()) {
        writeMode = ParquetJob.WriteMode.OVERWRITE;
      } else {
        writeMode = ParquetJob.WriteMode.APPEND;
        if (Datasets.exists(uri)) {
          LOG.warn("Target Hive table '" + tableName + "' exists! Sqoop will " +
              "append data into the existing Hive table. Consider using " +
              "--hive-overwrite, if you do NOT intend to do appending.");
        }
      }
    } else {
      // Note that there is no such an import argument for overwriting HDFS
      // dataset, so overwrite mode is not supported yet.
      // Sqoop's append mode means to merge two independent datasets. We
      // choose DEFAULT as write mode.
      writeMode = ParquetJob.WriteMode.DEFAULT;
    }
    ParquetJob.configureImportJob(conf, schema, uri, writeMode);
  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return ParquetImportMapper.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DatasetKeyOutputFormat.class;
  }

  private String getKiteUri(Configuration conf, SqoopOptions options, String tableName, Path destination) throws IOException {
    if (options.doHiveImport()) {
      String hiveDatabase = options.getHiveDatabaseName() == null ? "default" :
          options.getHiveDatabaseName();
      String hiveTable = options.getHiveTableName() == null ? tableName :
          options.getHiveTableName();
      return String.format("dataset:hive:/%s/%s", hiveDatabase, hiveTable);
    } else {
      return "dataset:" + FileSystemUtil.makeQualified(destination, conf);
    }
  }
}
