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
 * A definition for a search query.
 */
public class SearchRequestDefinitionImpl<T> implements SearchRequestDefinition<T> {

    private List<T> expressions;
    private FilterNode<T> filterNode;
    private List<? extends SortDefinition<T>> sorts;
    private List<T> groupByExpressions;
    private Long skip;
    private Integer take;

    public SearchRequestDefinitionImpl() { }

    public SearchRequestDefinitionImpl(List<T> expressions,
                                       FilterNode<T> filterNode,
                                       List<? extends SortDefinition<T>> sorts,
                                       List<T> groupByExpressions,
                                       Long skip,
                                       Integer take) {
        this.expressions = expressions;
        this.filterNode = filterNode;
        this.sorts = sorts;
        this.groupByExpressions = groupByExpressions;
        this.skip = skip;
        this.take = take;
    }

    public SearchRequestDefinitionImpl(List<T> expressions,
                                       FilterNode<T> filterNode,
                                       List<? extends SortDefinition<T>> sorts,
                                       List<T> groupByExpressions) {
        this(expressions, filterNode, sorts, groupByExpressions, null, null);
    }

    public SearchRequestDefinitionImpl(FilterNode<T> filterNode, List<? extends SortDefinition<T>> sorts) {
        this(null, filterNode, sorts, null);
    }

    public SearchRequestDefinitionImpl(List<T> expressions, FilterNode<T> filterNode, SortDefinition<T>... sorts) {
        this(expressions, filterNode, Arrays.asList(sorts), null);
    }

    public SearchRequestDefinitionImpl(FilterNode<T> filterNode, SortDefinition<T>... sorts) {
        this(filterNode, Arrays.asList(sorts));
    }

    public SearchRequestDefinitionImpl(List<T> expressions) {
        this(expressions, null, (List<? extends SortDefinition<T>>) null, null);
    }

    public SearchRequestDefinitionImpl(FilterNode<T> filterNode) {
        this(filterNode, (List<? extends SortDefinition<T>>)null);
    }

    public SearchRequestDefinitionImpl(SortDefinition<T>... sorts) {
        this(null, Arrays.asList(sorts));
    }

    /**
     * List of expressions to be obtained by query.
     *
     * {@link SearchRequestDefinition#getExpressions()}
     */
    @Override
    public List<T> getExpressions() {
        return expressions;
    }

    /**
     * {@link SearchRequestDefinition#getExpressions()}
     */
    public void setExpressions(List<T> expressions) {
        this.expressions = expressions;
    }

    /**
     * Filter to be applied to a search query.
     */
    @Override
    public FilterNode<T> getFilter() {
        return filterNode;
    }

    /**
     * {@link SearchRequestDefinition#getFilter()}
     */
    public void setFilter(FilterNode<T> filterNode) {
        this.filterNode = filterNode;
    }

    /**
     * Sorts to be applied to a search query.
     */
    @Override
    public List<? extends SortDefinition<T>> getSorts() {
        return sorts;
    }

    /**
     * {@link SearchRequestDefinition#getSorts()}
     */
    public void setSorts(List<? extends SortDefinition<T>> sorts) {
        this.sorts = sorts;
    }

    /**
     * List of expressions to be used in GROUP BY block.
     *
     * {@link SearchRequestDefinition#getGroupByExpressions()}
     */
    @Override
    public List<T> getGroupByExpressions() {
        return groupByExpressions;
    }

    /**
     * {@link SearchRequestDefinition#getGroupByExpressions()}
     */
    public void setGroupByExpressions(List<T> value) {
        groupByExpressions = value;
    }

    /**
     * The number of elements to skip.
     */
    @Override
    public Long getSkip() {
        return skip;
    }

    /**
     * {@link SearchRequestDefinition#getSkip()}
     */
    @Override
    public void setSkip(Long skip) {
        this.skip = skip;
    }

    /**
     * The maximum number of elements to return.
     */
    @Override
    public Integer getTake() {
        return take;
    }

    /**
     * {@link SearchRequestDefinition#getTake()}
     */
    @Override
    public void setTake(Integer take) {
        this.take = take;
    }

    @Override
    public String toString() {
        return "SearchRequestDefinitionImpl{" +
                "expressions=" + expressions +
                ", filterNode=" + filterNode +
                ", sorts=" + sorts +
                ", groupByExpressions=" + groupByExpressions +
                skip != null ? (", skip=" + skip) : "" +
                take != null ? (", take=" + take) : "" +
                '}';
    }
}