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

import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.AllNumericEncodingTypesTestMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;


public class EncodingTests extends BaseStreamReplicatorTests {


    @Timeout(10)
    @Test
    void numericEncodingTypesTest() throws InvocationTargetException, IllegalAccessException {
        AllNumericEncodingTypesTestMessage message = new AllNumericEncodingTypesTestMessage();
        message.fillMessage(10, false);

        Pair<DXTickStream, TableDeclaration> chSchemaByStream = loadAndReplicateData(message, AllNumericEncodingTypesTestMessage.class);
        Map<String, Object> values = selectAllValues(chSchemaByStream.getRight()).get(0);

        assertEqualsObjectFields(values, Map.of("", message));
    }


}