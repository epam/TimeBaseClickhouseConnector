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
 * Filter `or` operator.
 */
public class OrFilterDefinitionImpl<T> implements OrFilterDefinition<T> {
    private List<? extends FilterNode<T>> filters;

    public OrFilterDefinitionImpl() { }

    public OrFilterDefinitionImpl(List<? extends FilterNode<T>> orFilters) {
        this.filters = orFilters;
    }

    public OrFilterDefinitionImpl(FilterNode<T>... orFilters) {
        this(Arrays.asList(orFilters));
    }

    /**
     * List of filters. All filters are applied through the operator `or`.
     */
    @Override
    public List<? extends FilterNode<T>> getOr() {
        return filters;
    }

    /**
     * {@link OrFilterDefinition#getOr()}
     */
    public void setOr(List<? extends FilterNode<T>> or) {
        filters = or;
    }

    @Override
    public String toString() {
        return "OR {" +
                filters +
                '}';
    }
}