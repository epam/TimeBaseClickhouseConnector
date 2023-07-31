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
 * A package of results for a search query.
 */
public class ResultPackageImpl<T> implements ResultPackage<T> {
    private List<T> items;
    private boolean hasMore;

    public ResultPackageImpl() {  }

    public ResultPackageImpl(List<T> items, boolean hasMore) {
        this.items = items;
        this.hasMore = hasMore;
    }

    public ResultPackageImpl(List<T> items) {
        this(items, false);
    }

    /**
     * List of elements.
     */
    @Override
    public List<T> getItems() {
        return items;
    }

    /**
     * Flag indicating that the request reached the specified or default page size and that there are more orders available for this request.
     */
    @Override
    public boolean isHasMore() {
        return hasMore;
    }

    /**
     * {@link ResultPackage#getItems()}
     */
    @Override
    public void setItems(List<T> items) {
        this.items = items;
    }

    /**
     * {@link ResultPackage#isHasMore()}
     */
    @Override
    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }
}