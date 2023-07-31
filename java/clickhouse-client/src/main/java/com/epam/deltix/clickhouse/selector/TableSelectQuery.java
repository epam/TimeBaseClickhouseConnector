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
package com.epam.deltix.clickhouse.selector;

import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.selector.definitions.FilterNode;
import com.epam.deltix.clickhouse.selector.definitions.ResultPackage;
import com.epam.deltix.clickhouse.selector.definitions.SearchRequestDefinition;
import com.epam.deltix.clickhouse.selector.definitions.SortDefinition;
import com.epam.deltix.clickhouse.util.SelectQueryHelper;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.gflog.api.Log;
import org.springframework.jdbc.core.RowMapper;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TableSelectQuery {
    private static final Log LOG = LogFactory.getLog(TableSelectQuery.class);

    private final ClickhouseClient clickhouseClient;
    private final QuerySource querySource;

    private final Map<String, ColumnDeclaration> columnDeclarations;

    public TableSelectQuery(final ClickhouseClient clickhouseClient, QuerySource querySource) {

        this.clickhouseClient = clickhouseClient;
        this.querySource = querySource;

        columnDeclarations = querySource.getExpressions().stream().collect(Collectors.toMap(ColumnDeclaration::getDbColumnName, Function.identity()));
    }

    public QuerySource getQuerySource() {
        return querySource;
    }

    public <TEntry, TPackage extends ResultPackage<TEntry>> TPackage select(SearchRequestDefinition<String> searchRequest, RowMapper<TEntry> rowMapper,
                                                                            Supplier<TPackage> packageSupplier) {
        return select(searchRequest, rowMapper, packageSupplier, false);
    }

    public <TEntry, TPackage extends ResultPackage<TEntry>> TPackage selectDistinct(SearchRequestDefinition<String> searchRequest, RowMapper<TEntry> rowMapper,
                                                                                    Supplier<TPackage> packageSupplier) {
        return select(searchRequest, rowMapper, packageSupplier, true);
    }

    private <TEntry, TPackage extends ResultPackage<TEntry>> TPackage select(SearchRequestDefinition<String> searchRequest, RowMapper<TEntry> rowMapper,
                                                                             Supplier<TPackage> packageSupplier, boolean distinct) {
        FilterNode<String> filter = searchRequest.getFilter();
        List<? extends SortDefinition<String>> sorts = searchRequest.getSorts();
        Long skip = searchRequest.getSkip();
        Integer take = searchRequest.getTake();
        boolean hasMore = false;

        if (LOG.isDebugEnabled())
            LOG.debug()
                    .append("Query: filter: ")
                    .append(filter)
                    .append(", sorts: ")
                    .append(Arrays.toString(sorts == null ? new Object[0] : sorts.toArray()))
                    .append(", skip: ")
                    .append(skip)
                    .append(", take: ")
                    .append(take)
                    .append(distinct ? " distinct" : "")
                    .commit();

        if (take == null || take.intValue() > clickhouseClient.getSettings().getMaxRecordsForSelectQuery()) {
            LOG.info()
                    .append("Limit max query count to ")
                    .append(clickhouseClient.getSettings().getMaxRecordsForSelectQuery())
                    .append(" records (")
                    .append(take)
                    .append(" requested).")
                    .commit();

            take = clickhouseClient.getSettings().getMaxRecordsForSelectQuery();
        }

        TPackage result = packageSupplier.get();

        searchRequest.setTake(take + 1);

        List<TEntry> entries = SelectQueryHelper.buildAndExecuteQuery(
                querySource, searchRequest, columnDeclarations, distinct, clickhouseClient, rowMapper);

        int entriesSize = entries.size();
        if (entriesSize > take) {
            hasMore = true;
            entries.remove(entriesSize - 1);
        }

        result.setItems(entries);
        result.setHasMore(hasMore);

        return result;
    }

//    public <TEntry> TEntry selectOne(SearchRequestDefinition searchRequest, RowMapper<TEntry> rowMapper) {
//        FilterNode filter = searchRequest.getFilter();
//        List<? extends SortDefinition> sorts = searchRequest.getSorts();
//
//        if (LOG.isDebugEnabled())
//            LOG.debug()
//                    .append("Query one: filter: ")
//                    .append(filter)
//                    .append(", sorts: ")
//                    .append(Arrays.toString(sorts == null ? new Object[0] : sorts.toArray()))
//                    .commit();
//
//        int take = 1;
//        searchRequest.setSkip(0L);
//        searchRequest.setTake(take);
//
//        List<TEntry> entries = SelectQueryHelper.buildAndExecuteQuery(
//                querySource, searchRequest, columnDeclarations, false, clickhouseClient, rowMapper);
//
//        if (entries.size() > take)
//            throw new IllegalStateException("Multiple entries matching criteria found.");
//
//        return entries.isEmpty() ? null : entries.get(0);
//    }
}