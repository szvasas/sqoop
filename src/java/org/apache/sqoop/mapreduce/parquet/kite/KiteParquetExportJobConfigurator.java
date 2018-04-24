package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;

public class KiteParquetExportJobConfigurator implements ParquetExportJobConfigurator {
  
  @Override
  public void configureInputFormat(Job job, String tableName, String tableClassName, String splitByCol) {

  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return null;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return null;
  }
}
