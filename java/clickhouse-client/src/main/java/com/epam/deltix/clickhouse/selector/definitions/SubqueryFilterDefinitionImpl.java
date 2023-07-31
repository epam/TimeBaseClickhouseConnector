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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.epam.deltix.clickhouse.selector.filters.FilterType;

/**
 * A filter definition for a search subquery.
 */
public class SubqueryFilterDefinitionImpl<T> implements SubqueryFilterDefinition<T> {
    private T fieldExpression;
    private FilterType filterType;
    private SearchRequestDefinition<T> filterValue;

    public SubqueryFilterDefinitionImpl() { }

    public SubqueryFilterDefinitionImpl(T fieldExpression,
                                        FilterType filterType,
                                        SearchRequestDefinition<T> filterValue) {
        this.fieldExpression = fieldExpression;
        this.filterType = filterType;
        this.filterValue = filterValue;
    }

    /**
     * Field name for which the filter is applied.
     */
    @Override
    public T getFieldExpression() {
        return fieldExpression;
    }

    /**
     * {@link SubqueryFilterDefinition#getFieldExpression()}
     */
    @Override
    public void setFieldExpression(T fieldExpression) {
        this.fieldExpression = fieldExpression;
    }

    /**
     * The type of the filter.
     */
    @Override
    public FilterType getFilterType() {
        return filterType;
    }

    /**
     * {@link SubqueryFilterDefinition#getFilterType()}
     */
    @Override
    public void setFilterType(FilterType filterType) {
        this.filterType = filterType;
    }

    /**
     * A definition for a search subquery.
     */
    @Override
    public SearchRequestDefinition<T> getFilterValue() {
        return filterValue;
    }

    /**
     * {@link SubqueryFilterDefinition#getFilterValue()}
     */
    @Override
    @JsonDeserialize(as = SearchRequestDefinitionImpl.class)
    public void setFilterValue(SearchRequestDefinition<T> filterValue) {
        this.filterValue = filterValue;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", fieldExpression, filterType, filterValue);
    }
}