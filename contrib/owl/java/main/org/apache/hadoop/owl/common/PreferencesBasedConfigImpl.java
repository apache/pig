
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.prefs.*;



public class PreferencesBasedConfigImpl implements Config {

    private static final int RELOAD_PREFS_INTERVAL = 60000; // 60k milliseconds 
    // 1 minute reload interval for picking up new preferences
    private long timeToReload = 0;

    public Preferences getPreferencesNode(String nodeName) throws OwlException {
        Preferences p = Preferences.systemRoot().node(nodeName);
        if (System.currentTimeMillis() > timeToReload){
            return syncConfig(p);
        }
        return p;   
    }

    @Override
    public void loadFromFile(String prefsFile) throws OwlException{
        InputStream is;
        try {
            is = new BufferedInputStream(new FileInputStream(prefsFile));
            Preferences.importPreferences(is);
            is.close();
        } catch (FileNotFoundException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_LOAD_CONFIG,e);
        } catch (IOException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_LOAD_CONFIG,e);
        } catch (InvalidPreferencesFormatException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_LOAD_CONFIG,e);
        }
    }

    public void printConfig(String nodeName, PrintStream pout) throws OwlException{
        Preferences p = getPreferencesNode(nodeName);
        try {
            for (String key : p.keys()){
                pout.println(key + " : " + p.get(key, "[null]"));
            }
        } catch (BackingStoreException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_LOAD_CONFIG,e);
        }
    }

    public void saveToFile(String nodeName, String prefsFile) throws OwlException{
        try {
            OutputStream os = new BufferedOutputStream(new FileOutputStream(prefsFile));
            getPreferencesNode(nodeName).exportNode(os);
            os.close();
        } catch (BackingStoreException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_STORE_CONFIG,e);
        } catch (FileNotFoundException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_STORE_CONFIG,e);
        } catch (IOException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_STORE_CONFIG,e);
        }
    }

    public Preferences syncConfig(Preferences p) throws OwlException {
        try {
            p.sync();
            String prefFactory = System.getProperty("java.util.prefs.PreferencesFactory");
            if (prefFactory != null && prefFactory.equals("org.apache.hadoop.owl.common.StubPreferencesFactory") 
                    && StubPreferencesFactory.useFileBackedStubPreferences()){
                // force-loading here until we have a backing implementation that works
                try {
                    Preferences.importPreferences(new FileInputStream(StubPreferencesFactory.getPreferencesFile()));
                } catch (FileNotFoundException e) {
                    throw new OwlException(ErrorType.ERROR_UNABLE_TO_SYNC_PREFS,e);
                } catch (IOException e) {
                    throw new OwlException(ErrorType.ERROR_UNABLE_TO_SYNC_PREFS,e);
                } catch (InvalidPreferencesFormatException e) {
                    throw new OwlException(ErrorType.ERROR_UNABLE_TO_SYNC_PREFS,e);
                }
            }
            timeToReload = System.currentTimeMillis()+RELOAD_PREFS_INTERVAL;
        } catch (BackingStoreException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_SYNC_PREFS,e);
        }
        return p;
    }

    @Override
    public void setConfigParam(String nodeName, String key, String value) throws OwlException {
        Preferences p = getPreferencesNode(nodeName);
        if (value != null){ // set key to a value if specified
            p.put(key, value);
        }else{ // nuke key if value was null
            p.remove(key);
        }
        try {
            p.flush();
            syncConfig(p);
        } catch (BackingStoreException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_STORE_CONFIG,e);
        }
    }

    @Override
    public String getConfigParam(String nodeName, String key) throws OwlException {
        String retval = getPreferencesNode(nodeName).get(key, null);
        if (retval == null){
            throw new OwlException(ErrorType.ERROR_UNKNOWN_CONFIG_PARAMETER,key);
        }
        return retval;
    }
}
