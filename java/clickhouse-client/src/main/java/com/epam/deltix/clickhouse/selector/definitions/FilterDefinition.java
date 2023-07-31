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
package com.epam.deltix.clickhouse.selector.definitions;

import com.epam.deltix.clickhouse.selector.filters.FilterType;

import java.util.List;

/**
 * A filter definition for a search query.
 */
public interface FilterDefinition<T> extends FilterNode<T> {

    /**
     * Field name for which the filter is applied.
     */
    T getFieldExpression();

    /**
     * Field name for which the filter is applied.
     */
    void setFieldExpression(T value);

    /**
     * The type of the filter.
     */
    FilterType getFilterType();

    /**
     * The type of the filter.
     */
    void setFilterType(FilterType value);

    /**
     * The value of the filter.
     */
    List<String> getFilterValue();

    /**
     * The value of the filter.
     */
    void setFilterValue(List<String> value);
}