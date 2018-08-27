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

package org.apache.sqoop.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.DefaultS3CredentialGenerator;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.S3CredentialGenerator;
import org.apache.sqoop.testutil.S3TestUtils;
import org.apache.sqoop.util.FileSystemUtil;
import org.apache.sqoop.util.ParquetReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestS3IncrementalAppendParquetImport extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
            TestS3IncrementalAppendParquetImport.class.getName());

    private static S3CredentialGenerator s3CredentialGenerator;

    private FileSystem s3Client;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupS3Credentials() throws IOException {
        String generatorCommand = S3TestUtils.getGeneratorCommand();
        if (generatorCommand != null) {
            s3CredentialGenerator = new DefaultS3CredentialGenerator(generatorCommand);
        }
    }

    @Before
    public void setup() throws IOException {
        S3TestUtils.runTestCaseOnlyIfS3CredentialsAreSet(s3CredentialGenerator);
        super.setUp();
        S3TestUtils.createTestTableFromInputData(this);
        s3Client = S3TestUtils.setupS3ImportTestCase(s3CredentialGenerator);
    }

    @After
    public void cleanUpOutputDirectories() {
        S3TestUtils.cleanUpDirectory(s3Client, S3TestUtils.getTargetDirPath());
        S3TestUtils.cleanUpDirectory(s3Client, S3TestUtils.getTemporaryRootDirPath());
        S3TestUtils.resetTargetDirName();
        super.tearDown();
    }

    protected ArgumentArrayBuilder getArgumentArrayBuilder() {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForUnitTests(this, s3CredentialGenerator);
        return builder;
    }

    @Test
    public void testS3IncrementalAppendAsParquetFileWhenNoNewRowIsImported() throws Exception {
        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withOption("as-parquetfile");
        String[] args = builder.build();
        runImport(args);

        builder = S3TestUtils.addIncrementalAppendImportArgs(builder);
        args = builder.build();
        runImport(args);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();

        assertEquals(S3TestUtils.getExpectedParquetOutput(), result);
    }

    @Test
    public void testS3IncrementalAppendAsParquetFile() throws Exception {
        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withOption("as-parquetfile");
        String[] args = builder.build();
        runImport(args);

        S3TestUtils.insertInputDataIntoTable(this, S3TestUtils.getExtraInputData());

        builder = S3TestUtils.addIncrementalAppendImportArgs(builder);
        args = builder.build();
        runImport(args);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();

        assertEquals(S3TestUtils.getExpectedParquetOutputAfterAppend(), result);
    }

    @Test
    public void testS3IncrementalAppendAsParquetFileWithMapreduceOutputBasenameProperty() throws Exception {
        final String MAPREDUCE_OUTPUT_BASENAME_PROPERTY = "mapreduce.output.basename";
        final String MAPREDUCE_OUTPUT_BASENAME = "custom";

        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
        builder.withOption("as-parquetfile");
        String[] args = builder.build();
        runImport(args);

        S3TestUtils.insertInputDataIntoTable(this, S3TestUtils.getExtraInputData());

        builder = S3TestUtils.addIncrementalAppendImportArgs(builder);
        args = builder.build();
        runImport(args);

        List<Path> outputFilesWithCustomName = FileSystemUtil.findFilesWithPathContainingPattern(S3TestUtils.getTargetDirPath(), s3Client.getConf(), MAPREDUCE_OUTPUT_BASENAME);
        if (outputFilesWithCustomName.size() == 0) {
            fail("No output file with custom name was created.");
        }

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();

        assertEquals(S3TestUtils.getExpectedParquetOutputAfterAppend(), result);

        System.clearProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY);
    }

}
