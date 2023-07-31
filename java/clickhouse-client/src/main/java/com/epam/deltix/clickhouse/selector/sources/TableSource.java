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
package com.epam.deltix.clickhouse.selector.sources;

import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.selector.QuerySource;
import com.epam.deltix.clickhouse.selector.SelectBuilder;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class TableSource implements QuerySource {

    private final ClickhouseClient clickhouseClient;
    private final TableIdentity tableIdentity;

    private final List<ColumnDeclaration> columnDeclarations;

    public TableSource(final DataSource dataSource, final TableIdentity tableIdentity) {

        this.clickhouseClient = new ClickhouseClient(dataSource);
        this.tableIdentity = tableIdentity;

        columnDeclarations = new ArrayList<>(clickhouseClient.describeTable(tableIdentity).getColumns());
    }


    @Override
    public List<ColumnDeclaration> getExpressions() {
        return columnDeclarations;
    }

    @Override
    public void populateFrom(SelectBuilder selectBuilder) {
        selectBuilder.from(tableIdentity);
    }
}