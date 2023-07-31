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
package com.epam.deltix.timebase.connector.clickhouse.util;

import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.codec.FieldLayout;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassSet;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.SchemaOptions;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.WriteMode;

import java.util.HashMap;
import java.util.Map;

import static com.epam.deltix.timebase.connector.clickhouse.algos.Replicator.ALL_TYPES;

public class Util {

    public static SchemaOptions getSchemaOptions(DXTickStream stream, ColumnNamingScheme pattern) {
        RecordClassSet classSet = new RecordClassSet(stream.isFixedType() ?
                new RecordClassDescriptor[]{stream.getFixedType()} :
                stream.getPolymorphicDescriptors());
        Map<String, String> mapping =  new HashMap<>(){{put(ALL_TYPES, stream.getKey());}};
        return new SchemaOptions(classSet, mapping, WriteMode.APPEND, pattern, true);
    }

    public static StreamRequest getStreamRequest(DXTickStream stream, ColumnNamingScheme pattern) {
        StreamRequest streamRequest = new StreamRequest();
        streamRequest.setKey(stream.getKey());
        streamRequest.setStream(stream.getKey());
        streamRequest.setColumnNamingScheme(pattern);
        streamRequest.setIncludePartitionColumn(true);
        return streamRequest;
    }
    public static TableDeclaration getTableDeclaration(DXTickStream stream, ColumnNamingScheme pattern) {
        Map<String, TableDeclaration> tableDeclaration = getTableDeclaration(getSchemaOptions(stream, pattern));
        return tableDeclaration.get(ALL_TYPES);
    }

    public static TableDeclaration getTableDeclaration(DXTickStream stream) {
        return getTableDeclaration(stream,  ColumnNamingScheme.TYPE_AND_NAME);
    }

    public static Map<String, TableDeclaration> getTableDeclaration(SchemaOptions schemaOptions) {
        ClickhouseProperties clickhouseProperties = new ClickhouseProperties();
        clickhouseProperties.setDatabase("tbMessages");
        SchemaProcessor schemaProcessor = new SchemaProcessor(schemaOptions, null, clickhouseProperties);
        return schemaProcessor.timebaseStreamToClickhouseTable();
    }

    public static ColumnDeclarationEx getColumnDeclaration(RecordClassDescriptor descriptor, FieldLayout dataField, DXTickStream stream, ColumnNamingScheme scheme) {
        SchemaProcessor schemaProcessor = new SchemaProcessor(getSchemaOptions(stream, scheme), null, null);
        return schemaProcessor.getColumnDeclaration(descriptor, dataField);
    }
}