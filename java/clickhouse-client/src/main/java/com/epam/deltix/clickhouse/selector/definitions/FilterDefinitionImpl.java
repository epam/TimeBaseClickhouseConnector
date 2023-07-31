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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A filter definition for a search query.
 */
public class FilterDefinitionImpl<T> implements FilterDefinition<T> {
    private T fieldExpression;
    private FilterType filterType;
    private List<String> filterValue;

    public FilterDefinitionImpl() { }


    public FilterDefinitionImpl(T fieldExpression, FilterType filterType, List<String> filterValues) {
        this.fieldExpression = fieldExpression;
        this.filterType = filterType;
        this.filterValue = filterValues;
    }

    public FilterDefinitionImpl(T fieldExpression, FilterType filterType, String... filterValues) {
        this(fieldExpression, filterType, Arrays.asList(filterValues));
    }

    public FilterDefinitionImpl(T fieldExpression, FilterType filterType, String filterValue) {
        this(fieldExpression, filterType, Collections.singletonList(filterValue));
    }


    /**
     * Field name for which the filter is applied.
     */
    @Override
    public T getFieldExpression() {
        return fieldExpression;
    }

    /**
     * The type of the filter.
     */
    @Override
    public FilterType getFilterType() {
        return filterType;
    }

    /**
     * The value of the filter.
     */
    @Override
    public List<String> getFilterValue() {
        return filterValue;
    }

    /**
     * {@link FilterDefinition#getFieldExpression()}
     */
    public void setFieldExpression(T fieldExpression) {
        this.fieldExpression = fieldExpression;
    }

    /**
     * {@link FilterDefinition#getFilterType()}
     */
    public void setFilterType(FilterType filterType) {
        this.filterType = filterType;
    }

    /**
     * {@link FilterDefinition#getFilterValue()}
     */
    public void setFilterValue(List<String> filterValue) {
        this.filterValue = filterValue;
    }

//    public void setFilterValue(String... filterValue) {
//        this.filterValue = Arrays.asList(filterValue);
//    }

    @Override
    public String toString() {
        return String.format("%s %s %s", fieldExpression, filterType, filterValue);
    }
}