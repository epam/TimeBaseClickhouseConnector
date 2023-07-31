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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Filter `or` operator.
 */
public interface OrFilterDefinition<T> extends FilterNode<T> {
    /**
     * List of filters. All filters are applied through the operator `or`.
     */
    @JsonProperty("filters")
    List<? extends FilterNode<T>> getOr();

    /**
     * List of filters. All filters are applied through the operator `or`.
     */
    @JsonProperty("filters")
    void setOr(List<? extends FilterNode<T>> value);
}