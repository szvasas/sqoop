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
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.DefaultS3CredentialGenerator;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.S3CredentialGenerator;
import org.apache.sqoop.testutil.S3TestUtils;
import org.apache.sqoop.testutil.TextFileTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class TestS3IncrementalMergeTextImport extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
            TestS3IncrementalMergeTextImport.class.getName());

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
        S3TestUtils.createTestTableFromInitialInputDataForMerge(this);
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
    public void testS3IncrementalMergeAsTextFileWhenNoNewRowIsImported() throws Exception {
        final String REDUCE_OUTPUT_FILE_00000 = S3TestUtils.MAPREDUCE_OUTPUT_PART + S3TestUtils.REDUCE_OUTPUT_00000;

        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        String[] args = builder.build();
        runImport(args);
        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutputBeforeMerge(), s3Client, S3TestUtils.getTargetDirPath());

        clearTable(getTableName());

        builder = S3TestUtils.addIncrementalMergeImportArgs(builder);
        args = builder.build();
        runImport(args);

        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutputBeforeMerge(), s3Client, S3TestUtils.getTargetDirPath(), REDUCE_OUTPUT_FILE_00000);
    }

    @Test
    public void testS3IncrementalMergeAsTextFile() throws Exception {
        final String REDUCE_OUTPUT_FILE_00000 = S3TestUtils.MAPREDUCE_OUTPUT_PART + S3TestUtils.REDUCE_OUTPUT_00000;

        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        String[] args = builder.build();
        runImport(args);
        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutputBeforeMerge(), s3Client, S3TestUtils.getTargetDirPath());

        clearTable(getTableName());

        S3TestUtils.insertInputDataIntoTableForMerge(this, S3TestUtils.getNewInputDataForMerge());

        builder = S3TestUtils.addIncrementalMergeImportArgs(builder);
        args = builder.build();
        runImport(args);

        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutputAfterMerge(), s3Client, S3TestUtils.getTargetDirPath(), REDUCE_OUTPUT_FILE_00000);
    }

    @Test
    public void testS3IncrementalMergeAsTextFileWithMapreduceOutputBasenameProperty() throws Exception {
        final String MAPREDUCE_OUTPUT_BASENAME_PROPERTY = "mapreduce.output.basename";
        final String MAPREDUCE_OUTPUT_BASENAME = "custom";
        final String MAP_OUTPUT_FILE_00000 = MAPREDUCE_OUTPUT_BASENAME + S3TestUtils.MAP_OUTPUT_00000;
        final String REDUCE_OUTPUT_FILE_00000 = MAPREDUCE_OUTPUT_BASENAME + S3TestUtils.REDUCE_OUTPUT_00000;

        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
        String[] args = builder.build();
        runImport(args);
        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutputBeforeMerge(), s3Client, S3TestUtils.getTargetDirPath(), MAP_OUTPUT_FILE_00000);

        clearTable(getTableName());

        S3TestUtils.insertInputDataIntoTableForMerge(this, S3TestUtils.getNewInputDataForMerge());

        builder = S3TestUtils.addIncrementalMergeImportArgs(builder);
        args = builder.build();
        runImport(args);

        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutputAfterMerge(), s3Client, S3TestUtils.getTargetDirPath(), REDUCE_OUTPUT_FILE_00000);

        System.clearProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY);
    }
}
