/*
 * Copyright 2023 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.clickhouse;

public class ClickhouseClientSettings {

    public static ClickhouseClientSettings DEFAULT = new ClickhouseClientSettings();

    private int maxRecordsForSelectQuery = 5_000;
    private boolean saveGeneratedClasses = false;
    private String generatedClassPath = ".appdata/byteBuddy";

    public int getMaxRecordsForSelectQuery() {
        return maxRecordsForSelectQuery;
    }

    public void setMaxRecordsForSelectQuery(int maxRecordsForSelectQuery) {
        this.maxRecordsForSelectQuery = maxRecordsForSelectQuery;
    }

    public boolean isSaveGeneratedClasses() {
        return saveGeneratedClasses;
    }

    public void setSaveGeneratedClasses(boolean saveGeneratedClasses) {
        this.saveGeneratedClasses = saveGeneratedClasses;
    }

    public String getGeneratedClassPath() {
        return generatedClassPath;
    }

    public void setGeneratedClassPath(String generatedClassPath) {
        this.generatedClassPath = generatedClassPath;
    }
}