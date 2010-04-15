
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

package org.apache.hadoop.owl.common;

import java.io.File;
import java.util.prefs.Preferences;
import java.util.prefs.PreferencesFactory;


public class StubPreferencesFactory implements PreferencesFactory {

    public static final String PROPERTY_STUB_PREFERENCES_FILE = "org.apache.hadoop.owl.preferences";


    private static final boolean useFileBackedStubPreferences = true;

    private static Preferences p = null;  // using the same preferences root for both systemRoot and userRoot
    private static File preferencesFile = null;

    private static String getPreferencesFileName() {
        return System.getProperty(PROPERTY_STUB_PREFERENCES_FILE);
    }

    public static File getPreferencesFile(){
        if (preferencesFile == null){
            String fileName = getPreferencesFileName();
            preferencesFile = new File(fileName);
        }
        return preferencesFile;
    }

    public static boolean useFileBackedStubPreferences(){
        String fileName = getPreferencesFileName();
        return (fileName != null)&&(fileName.length() > 0)&&(useFileBackedStubPreferences);
    }

    private static void instantiatePreferences(){
        // instantiate a StubPreferences (in-memory preferences with no backing store) if no preferences file is specified
        // instantiate a file-backed preferences node if it is specified -- not yet implemented as a separate class
        p = new StubPreferences(null,"");
    }

    @Override
    public Preferences systemRoot() {
        if (p == null){
            instantiatePreferences();
        }
        return p;
    }

    @Override
    public Preferences userRoot() {
        if (p == null){
            instantiatePreferences();
        }
        return p;
    }

}
