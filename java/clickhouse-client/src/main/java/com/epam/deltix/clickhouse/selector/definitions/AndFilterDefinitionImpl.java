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

import java.util.Arrays;
import java.util.List;

/**
 * Filter `and` operator.
 */
public class AndFilterDefinitionImpl<T> implements AndFilterDefinition<T> {
    private List<? extends FilterNode<T>> filters;

    public AndFilterDefinitionImpl() { }

    public AndFilterDefinitionImpl(List<? extends FilterNode<T>> andFilters) {
        this.filters = andFilters;
    }

    public AndFilterDefinitionImpl(FilterNode<T>... andFilters) {
        this(Arrays.asList(andFilters));
    }


    /**
     * List of filters. All filters are applied through the operator `and`.
     */
    @Override
    public List<? extends FilterNode<T>> getAnd() {
        return filters;
    }

    /**
     * {@link AndFilterDefinition#getAnd()}
     */
    public void setAnd(List<? extends FilterNode<T>> and) {
        filters = and;
    }


    @Override
    public String toString() {
        return "AND {" +
                filters +
                '}';
    }
}