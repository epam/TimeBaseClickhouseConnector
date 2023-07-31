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


import java.util.List;

/**
 * A definition for a search query.
 */
public interface SearchRequestDefinition<T> {

    /**
     * List of expressions to be obtained by query.
     *
     * An expression is a combination of one or more values, operators and functions that evaluate to a value.
     * These expressions are like formula and they are written in query language.
     * Expression is a combination of one or more fields (columns), constants, operators (arithmetic and logical), and function calls.
     *
     * Expression can be associated with alias using `AS` keyword.
     * Example: 10 + 5 AS mySum, where `10 + 5` - expression, `mySum` - alias associated with the expression.
     *
     * Aliases are often used to make expressions more readable. An alias only exists in the context of the query.
     *
     * Aliases can be used in search expressions, grouping expressions, filters and sorts.
     */
    List<T> getExpressions();

    /**
     * List of expressions to be obtained by query.
     */
    void setExpressions(List<T> value);

    /**
     * Filter to be applied to a search query.
     */
    FilterNode<T> getFilter();

    /**
     * Filter to be applied to a search query.
     */
    void setFilter(FilterNode<T> value);

    /**
     * Sorts to be applied to a search query.
     */
    List<? extends SortDefinition<T>> getSorts();

    /**
     * Sorts to be applied to a search query.
     */
    void setSorts(List<? extends SortDefinition<T>> value);

    /**
     * List of expressions to be used in GROUP BY block.
     *
     * Grouping expressions used to arrange identical data into groups.
     * It is typically used in conjunction with aggregate functions such as `sum` or `count` to aggregate values for each group.
     * Only the unique combinations are returned.
     * Each grouping expression will be referred to here as a `key`.
     * All the expressions in the search expressions and sorts clauses must be calculated from `keys` or from aggregate functions.
     */
    List<T> getGroupByExpressions();

    /**
     * List of expressions to be used in GROUP BY block.
     */
    void setGroupByExpressions(List<T> value);

    /**
     * The number of elements to skip.
     */
    Long getSkip();

    /**
     * The number of elements to skip.
     */
    void setSkip(Long skip);

    /**
     * The maximum number of elements to return.
     */
    Integer getTake();

    /**
     * The maximum number of elements to return.
     */
    void setTake(Integer take);
}