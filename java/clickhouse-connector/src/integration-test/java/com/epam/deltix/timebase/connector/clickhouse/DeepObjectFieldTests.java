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
package com.epam.deltix.timebase.connector.clickhouse;

import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.codec.CodecFactory;
import com.epam.deltix.qsrv.hf.pub.codec.UnboundDecoder;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.tickdb.pub.SelectionOptions;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickCursor;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.timebase.TestEnum;
import com.epam.deltix.timebase.connector.clickhouse.timebase.callinfo.CallInfo;
import com.epam.deltix.timebase.connector.clickhouse.timebase.callinfo.CallsSnapshot;
import com.epam.deltix.timebase.connector.clickhouse.timebase.callinfo.LocationQuantity;
import com.epam.deltix.timebase.connector.clickhouse.timebase.deepObj.L1;
import com.epam.deltix.timebase.connector.clickhouse.timebase.deepObj.L2;
import com.epam.deltix.timebase.connector.clickhouse.timebase.deepObj.L3;
import com.epam.deltix.timebase.connector.clickhouse.timebase.deepObj.L4;
import com.epam.deltix.timebase.connector.clickhouse.util.Util;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.algos.UnboundTableWriter;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.SchemaOptions;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.util.collections.generated.ObjectArrayList;
import com.epam.deltix.util.memory.MemoryDataInput;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DeepObjectFieldTests extends BaseStreamReplicatorTests {


    @Timeout(10)
    @Test
    void readCallsObject() throws SQLException {

        CallsSnapshot message = new CallsSnapshot();
        ObjectArrayList<InstrumentMessage> calls = new ObjectArrayList<>();
        CallInfo callInfo = new CallInfo();
        initSystemRequiredFields(callInfo);
        callInfo.CallId = "CallId";
        ObjectArrayList<InstrumentMessage> locations = new ObjectArrayList<>();
        locations.add(new LocationQuantity(new ObjectArrayList<>(new CharSequence[]{"l1", "l11"}), 999, "location1", TestEnum.FIRST));
        locations.add(new LocationQuantity(new ObjectArrayList<>(new CharSequence[]{"l2", "l22"}), 111, "location2", TestEnum.SECOND));
        callInfo.Locations = locations;
        calls.add(callInfo);
        calls.add(callInfo);
        message.Calls = calls;
        initSystemRequiredFields(message);
        DXTickStream stream = loadData(message, CallsSnapshot.class, CallInfo.class, LocationQuantity.class);

        SchemaOptions schemaOptions = Util.getSchemaOptions(stream, ColumnNamingScheme.TYPE_AND_NAME);
        ClickhouseProperties clickhouseProperties = new ClickhouseProperties();
        clickhouseProperties.setDatabase("tbMessages");
        SchemaProcessor schemaProcessor = new SchemaProcessor(schemaOptions, clickhouseClient , clickhouseProperties);
        Map<String, TableDeclaration> stringTableDeclarationMap = schemaProcessor.prepareClickhouseTable();
        MemoryDataInput in = new MemoryDataInput();
        RecordClassDescriptor[] descriptors = schemaOptions.getTbSchema().getContentClasses();
        List<UnboundDecoder> decoders = Arrays.stream(descriptors).map(CodecFactory.COMPILED::createFixedUnboundDecoder).collect(Collectors.toList());

        UnboundTableWriter unboundTableWriter = new UnboundTableWriter(stream.getKey(), ColumnNamingScheme.TYPE_AND_NAME, clickhouseClient,
                stringTableDeclarationMap, schemaProcessor.getColumnDeclarations(), decoders, in/*, 10_000, 5_000*/);

        TickCursor cursor = stream.select(0, new SelectionOptions(true, false));
        cursor.next();

        assertThrows(UnsupportedOperationException.class, () -> unboundTableWriter.send((RawMessage) cursor.getMessage(), cursor));

    }

    @Timeout(10)
    @Test
    void readFourLevelObject() {

        L1 message = new L1(
                new L2(new L3(new L4("l41", TestEnum.FIRST), "l3", List.of(TestEnum.FIRST.name())), "l2",
                        List.of(new L3(new L4("l42", TestEnum.SECOND), "l3ar1", List.of(TestEnum.FIRST.name(), TestEnum.SECOND.name())),
                                new L3(new L4("l42", TestEnum.DEFAULT), "l3arr2", List.of(TestEnum.FIRST.name(), TestEnum.DEFAULT.name()))
                        )
                ),
                "l1");

        initSystemRequiredFields(message);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message, L1.class, L2.class, L3.class, L4.class);
        TableDeclaration tableDeclaration = chSchemaByStream.getRight();
        Map<String, Object> values = selectAllValues(tableDeclaration).get(0);
        Map<String, Object> expectedValues = new HashMap<>() {{
            put("type", "deltix.clickhouse.timebase.L1");
            put("L1_l1simple", "l1");
            put("L1_l1obj_type", "deltix.clickhouse.timebase.L2");
            put("L1_l1obj_L2_l2obj_type", "deltix.clickhouse.timebase.L3");
            put("L1_l1obj_L2_l2simple", "l2");
            put("L1_l1obj_L2_l2obj_L3_l3simple", "l3");
            put("L1_l1obj_L2_l2obj_L3_l3obj_L4_l4simple", "l41");
            put("L1_l1obj_L2_l2obj_L3_l3obj_L4_enumField", TestEnum.FIRST.name());
            put("L1_l1obj_L2_l2obj_L3_enumArray", new String[]{TestEnum.FIRST.name()});
            put("L1_l1obj_L2_l2obj_L3_l3obj_type", "deltix.clickhouse.timebase.L4");
            put("L1_l1obj_L2_l2ArrObj.type", new String[]{"deltix.clickhouse.timebase.L3", "deltix.clickhouse.timebase.L3"});
            put("L1_l1obj_L2_l2ArrObj.L3_l3obj_type", new String[]{"deltix.clickhouse.timebase.L4", "deltix.clickhouse.timebase.L4"});
            put("L1_l1obj_L2_l2ArrObj.L3_l3obj_L4_l4simple", new String[]{"l42", "l42"});
            put("L1_l1obj_L2_l2ArrObj.L3_l3obj_L4_enumField", new String[]{TestEnum.SECOND.name(), TestEnum.DEFAULT.name()});
            put("L1_l1obj_L2_l2ArrObj.L3_enumArray", new String[][]{new String[]{TestEnum.FIRST.name(), TestEnum.SECOND.name()}, new String[]{TestEnum.FIRST.name(), TestEnum.DEFAULT.name()}});
            put("L1_l1obj_L2_l2ArrObj.L3_l3simple", new String[]{"l3ar1", "l3arr2"});
        }};
        expectedValues.forEach((k, v) -> {
            Object actualValue = values.get(k);
            assertArrayEquals(new Object[]{v}, new Object[]{actualValue});
        });
    }

}