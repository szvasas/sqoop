package org.apache.sqoop.mapreduce.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.mapreduce.parquet.hadoop.HadoopParquetJobConfiguratorFactory;
import org.apache.sqoop.mapreduce.parquet.kite.KiteParquetJobConfiguratorFactory;

public final class ParquetJobConfiguratorFactoryCreator {

  private ParquetJobConfiguratorFactoryCreator() {
    // This class is meant for static use only.
  }

  public static ParquetJobConfiguratorFactory createParquetJobConfiguratorFactory(Configuration configuration) {
    return new HadoopParquetJobConfiguratorFactory();
  }

}
