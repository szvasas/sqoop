package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;
import org.apache.sqoop.util.FileSystemUtil;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;

import java.io.IOException;

public class KiteParquetExportJobConfigurator implements ParquetExportJobConfigurator {

  @Override
  public void configureInputFormat(Job job, Path inputPath) throws IOException {
    String uri = "dataset:" + FileSystemUtil.makeQualified(inputPath, job.getConfiguration());
    DatasetKeyInputFormat.configure(job).readFrom(uri);
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DatasetKeyInputFormat.class;
  }
}
