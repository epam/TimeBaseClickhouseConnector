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
public interface ResultPackage<T> {

    /**
     * List of elements.
     */
    List<T> getItems();

    /**
     * List of elements.
     */
    void setItems(List<T> items);

    /**
     * Flag indicating that the request reached the specified or default page size and that there are more orders available for this request.
     */
    boolean isHasMore();

    /**
     * Flag indicating that the request reached the specified or default page size and that there are more orders available for this request.
     */
    void setHasMore(boolean hasMore);
}