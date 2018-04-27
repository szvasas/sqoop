package org.apache.sqoop.mapreduce.parquet.hadoop;

import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;
import org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorFactory;
import org.apache.sqoop.mapreduce.parquet.ParquetMergeJobConfigurator;

public class HadoopParquetJobConfiguratorFactory implements ParquetJobConfiguratorFactory {

  @Override
  public ParquetImportJobConfigurator createParquetImportJobConfigurator() {
    return new HadoopParquetImportJobConfigurator();
  }

  @Override
  public ParquetExportJobConfigurator createParquetExportJobConfigurator() {
    return null;
  }

  @Override
  public ParquetMergeJobConfigurator createParquetMergeJobConfigurator() {
    return null;
  }
}
