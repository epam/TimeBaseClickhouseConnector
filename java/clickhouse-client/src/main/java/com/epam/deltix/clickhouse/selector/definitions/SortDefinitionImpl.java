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

import com.epam.deltix.clickhouse.selector.filters.SortType;

/**
 * A sort definition for a search query.
 */
public class SortDefinitionImpl<T> implements SortDefinition<T> {
    private T fieldName;
    private SortType sortType = SortType.ASC;


    public SortDefinitionImpl() { }

    public SortDefinitionImpl(T fieldName, SortType sortType) {
        this.fieldName = fieldName;
        this.sortType = sortType;
    }

    public SortDefinitionImpl(T fieldName) {
        this(fieldName, SortType.ASC);
    }

    /**
     * Field name or expression for which the sort is applied.
     */
    @Override
    public T getFieldName() {
        return fieldName;
    }

    /**
     * The type of the sort.
     */
    @Override
    public SortType getSortType() {
        return sortType;
    }

    /**
     * {@link SortDefinition#getFieldName()}
     */
    public void setFieldName(T fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * {@link SortDefinition#getSortType()}
     */
    public void setSortType(SortType sortType) {
        this.sortType = sortType;
    }

    @Override
    public String toString() {
        return String.format("%s %s", fieldName, sortType);
    }
}