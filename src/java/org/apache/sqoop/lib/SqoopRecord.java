/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.lib;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.sqoop.mapreduce.DBWritable;

/**
 * Interface implemented by the classes generated by sqoop's orm.ClassWriter.
 */
public abstract class SqoopRecord implements Cloneable, DBWritable,
    org.apache.sqoop.lib.FieldMappable, Writable  {

  public SqoopRecord() {
  }


  public abstract void parse(CharSequence s)
      throws com.cloudera.sqoop.lib.RecordParser.ParseError;
  public abstract void parse(Text s)
      throws com.cloudera.sqoop.lib.RecordParser.ParseError;
  public abstract void parse(byte [] s)
      throws com.cloudera.sqoop.lib.RecordParser.ParseError;
  public abstract void parse(char [] s)
      throws com.cloudera.sqoop.lib.RecordParser.ParseError;
  public abstract void parse(ByteBuffer s)
      throws com.cloudera.sqoop.lib.RecordParser.ParseError;
  public abstract void parse(CharBuffer s)
      throws com.cloudera.sqoop.lib.RecordParser.ParseError;
  public abstract void loadLargeObjects(
      com.cloudera.sqoop.lib.LargeObjectLoader objLoader)
      throws SQLException, IOException, InterruptedException;

  /**
   * Inserts the data in this object into the PreparedStatement, starting
   * at parameter 'offset'.
   * @return the number of fields written to the statement.
   */
  public abstract int write(PreparedStatement stmt, int offset)
      throws SQLException;

  /**
   * Format output data according to the specified delimiters.
   */
  public abstract String toString(
      com.cloudera.sqoop.lib.DelimiterSet delimiters);

  /**
   * Use the default delimiters, but only append an end-of-record delimiter
   * if useRecordDelim is true.
   */
  public String toString(boolean useRecordDelim) {
    // Method body should be overridden by generated classes in 1.3.0+
    if (useRecordDelim) {
      // This is the existing functionality.
      return toString();
    } else {
      // Setting this to false requires behavior in the generated class.
      throw new RuntimeException(
          "toString(useRecordDelim=false) requires a newer SqoopRecord. "
          + "Please regenerate your record class to use this function.");
    }
  }

  /**
   * Format the record according to the specified delimiters. An end-of-record
   * delimiter is optional, and only used if useRecordDelim is true. For
   * use with TextOutputFormat, calling this with useRecordDelim=false may
   * make more sense.
   */
  public String toString(
      com.cloudera.sqoop.lib.DelimiterSet delimiters, boolean useRecordDelim) {
    if (useRecordDelim) {
      return toString(delimiters);
    } else {
      // Setting this to false requires behavior in the generated class.
      throw new RuntimeException(
          "toString(delimiters, useRecordDelim=false) requires a newer "
          + "SqoopRecord. Please regenerate your record class to use this "
          + "function.");
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * Returns an integer specifying which API format version the
   * generated class conforms to. Used by internal APIs for backwards
   * compatibility.
   * @return the API version this class was generated against.
   */
  public abstract int getClassFormatVersion();

  /**
   * Use the delegate pattern to allow arbitrary processing of the
   * fields of this record.
   * @param processor A delegate that operates on this object.
   * @throws IOException if the processor encounters an IO error when
   * operating on this object.
   * @throws com.cloudera.sqoop.lib.ProcessingException if the FieldMapProcessor
   * encounters a general processing error when operating on this object.
   */
  public void delegate(org.apache.sqoop.lib.FieldMapProcessor processor)
      throws IOException, com.cloudera.sqoop.lib.ProcessingException {
    processor.accept(this);
  }

  @Override
  /**
   * {@inheriDoc}
   * @throws RuntimeException if used with a record that was generated
   * before this capability was added (1.1.0).
   */
  public Map<String, Object> getFieldMap() {
    // Default implementation does not support field iteration.
    // ClassWriter should provide an overriding version.
  throw new RuntimeException(
      "Got null field map from record. Regenerate your record class.");
  }

  /**
   * Allows an arbitrary field to be set programmatically to the
   * specified value object. The value object must match the
   * type expected for the particular field or a RuntimeException
   * will result.
   * @throws RuntimeException if the specified field name does not exist.
   */
  public void setField(String fieldName, Object fieldVal) {
    throw new RuntimeException("This SqoopRecord does not support setField(). "
        + "Regenerate your record class.");
  }

}
