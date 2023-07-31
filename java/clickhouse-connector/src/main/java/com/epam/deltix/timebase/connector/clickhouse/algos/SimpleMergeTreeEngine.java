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
package com.epam.deltix.timebase.connector.clickhouse.algos;

import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.engines.Engine;

import java.util.List;

public class SimpleMergeTreeEngine implements Engine {

    private static final String MERGE_TREE_ENGINE_NAME = "MergeTree";
    private static final int DEFAULT_GRANULARITY_INDEX = 8192;

    private final List<ColumnDeclaration> primaryKeyColumns;
    private final int granularityIndex;

    public SimpleMergeTreeEngine(List<ColumnDeclaration> primaryKeyColumns) {
        this(primaryKeyColumns, DEFAULT_GRANULARITY_INDEX);
    }

    public SimpleMergeTreeEngine(List<ColumnDeclaration> primaryKeyColumns, int granularityIndex) {
        if (primaryKeyColumns == null || primaryKeyColumns.size() == 0)
            throw new IllegalArgumentException("Primary key columns are not defined.");
        if (granularityIndex <= 0)
            throw new IllegalArgumentException("granularityIndex must be greater than zero.");

        this.primaryKeyColumns = primaryKeyColumns;
        this.granularityIndex = granularityIndex;
    }

    @Override
    public String getSqlDefinition() {
        StringBuilder sb = new StringBuilder(MERGE_TREE_ENGINE_NAME);

        sb.append("() order by (");

        for (int i = 0; i < primaryKeyColumns.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append(primaryKeyColumns.get(i).getDbColumnName());
        }

        sb.append(") settings index_granularity = ");
        sb.append(granularityIndex);

        return sb.toString();
    }

    @Override
    public String getName() {
        return MERGE_TREE_ENGINE_NAME;
    }
}