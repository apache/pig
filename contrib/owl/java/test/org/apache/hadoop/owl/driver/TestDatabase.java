/*
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
package org.apache.hadoop.owl.driver;

import java.util.List;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.TestOwlUtil;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDatabase {

    private static String dbname = "testdriverdb";
    private static OwlDriver driver;
    private static String dbLocation = "hdfs://localhost:9000/data/testdatabase";


    public TestDatabase() {
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        driver = new OwlDriver(OwlTestCase.getUri());
        DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);
        driver.createOwlDatabase(new OwlDatabase(dbname, dbLocation));
    }
    @Test
    public void testDatabaseName()  {
        // negative test for db name is longer than 64 characters.
        String dbLocation2 = "hdfs://localhost:9000/data/testdatabase2";
        String dbname_negativecase = TestOwlUtil.generateLongString(OwlUtil.IDENTIFIER_LIMIT +1);
        try{
            driver.createOwlDatabase(new OwlDatabase(dbname_negativecase, dbLocation2));
            Assert.fail("There is no OwlException thrown. We are expecting databasename variable length validation fails");
        }catch(OwlException e){
            Assert.assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
    }
    @Test
    public void testDatabaseLocationUniqueness(){
        String dbname_temp = "tempdb";
        // negative test to test db location is not unique
        try{
            driver.createOwlDatabase(new OwlDatabase(dbname_temp, dbLocation));
            Assert.fail("There is no OwlException thrown.  We are expecting database location uniqueness validation fails");
        }catch(OwlException e){
            Assert.assertTrue((e.getErrorType() == ErrorType.ERROR_RESOURCE_CREATE)|| (e.getErrorType() == ErrorType.ERROR_UNIQUE_KEY_CONSTRAINT));
        }
    }
    @Test
    public void testDatabaseLocationLength(){
        String dbname_temp2 = "tempdb2";
        //negative test to test db location length is limited to its limit
        String dbLocation_negativecase = TestOwlUtil.generateLongString(OwlUtil.LOCATION_LIMIT +1);
        try{
            driver.createOwlDatabase(new OwlDatabase(dbname_temp2, dbLocation_negativecase));
            Assert.fail("There is no OwlException thrown.  We are expecting database location variable length validation fails");
        }catch(OwlException e){
            Assert.assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
    }

    @Test
    public void testFetch() throws OwlException {
        OwlDatabase db = driver.getOwlDatabase(dbname);
        Assert.assertEquals(dbname, db.getName());
        Assert.assertEquals(dbLocation, db.getStorageLocation());
    }

    @Test
    public void testSelectAll() throws OwlException {
        List<String> allDbs = driver.showOwlDatabases();
        Assert.assertTrue(allDbs.contains(dbname));
    }

    @SuppressWarnings("null")
    @Test
    public void testInvalidCreate1() throws OwlException {
        OwlException exc = null;
        try {
            driver.createOwlDatabase(new OwlDatabase(dbname, null));
        } catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.INVALID_STORAGE_LOCATION, exc.getErrorType());
    }

    @SuppressWarnings("null")
    @Test
    public void testInvalidCreate2() throws OwlException {
        OwlException exc = null;
        try {
            driver.createOwlDatabase(null);
        } catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.INVALID_FIELD_VALUE, exc.getErrorType());
    }

    @SuppressWarnings("null")
    @Test
    public void testInvalidCreate3() throws OwlException {
        OwlException exc = null;
        try {
            driver.createOwlDatabase(new OwlDatabase(null, null));
        } catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.INVALID_FIELD_VALUE, exc.getErrorType());
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        OwlDatabase db = driver.getOwlDatabase(dbname);
        driver.dropOwlDatabase(db);
        List<String> allDbs = driver.showOwlDatabases();
        Assert.assertFalse(allDbs.contains(dbname));
    }

}
