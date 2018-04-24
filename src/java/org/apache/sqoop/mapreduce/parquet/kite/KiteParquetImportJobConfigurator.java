package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;

public class KiteParquetImportJobConfigurator implements ParquetImportJobConfigurator {
  
  @Override
  public void configureMapper(Job job, String tableName, String tableClassName) {

  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return null;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return null;
  }
}
