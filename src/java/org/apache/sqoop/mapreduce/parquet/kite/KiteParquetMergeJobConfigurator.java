package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.mapreduce.parquet.ParquetMergeJobConfigurator;

public class KiteParquetMergeJobConfigurator implements ParquetMergeJobConfigurator {
  
  @Override
  public void configueParquetMergeJob(Configuration conf, Job job, Path oldPath, Path newPath, Path finalPath) {

  }
}
