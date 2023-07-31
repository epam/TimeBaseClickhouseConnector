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
package com.epam.deltix.clickhouse.unit;

import com.epam.deltix.clickhouse.models.ClickhouseTableIdentity;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.types.Int64DataType;
import com.epam.deltix.clickhouse.schema.types.NestedDataType;
import com.epam.deltix.clickhouse.schema.types.StringDataType;
import com.epam.deltix.clickhouse.util.SqlQueryHelper;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SqlQueryHelperTests {

    @Test
    public void getInsertIntoQuery_expectCorrectQuery() {
        TableIdentity tableIdentity = ClickhouseTableIdentity.of("test");

        List<ColumnDeclaration> nestedColumns = Arrays.asList(
                new ColumnDeclaration("nested1", new StringDataType()),
                new ColumnDeclaration("nested2", new Int64DataType())
        );

        List<ColumnDeclaration> columns = Arrays.asList(
                new ColumnDeclaration("col1", new StringDataType()),
                new ColumnDeclaration("nested", new NestedDataType(nestedColumns))
        );

        String expected = "INSERT INTO test (`col1`, `nested.nested1`, `nested.nested2`) VALUES(?, ?, ?)";
        String actual = SqlQueryHelper.getInsertIntoQuery(tableIdentity, columns);

        Assert.assertEquals(expected, actual);
    }
}