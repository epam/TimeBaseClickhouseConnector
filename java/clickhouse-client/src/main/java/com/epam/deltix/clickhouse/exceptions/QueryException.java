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
package com.epam.deltix.clickhouse.exceptions;

import com.clickhouse.client.ClickHouseException;

public class QueryException extends RuntimeException {

    private final int errorCode;
    private final String displayText;

    public QueryException(int errorCode, String displayText) {
        this.errorCode = errorCode;
        this.displayText = displayText;
    }

    public QueryException(ClickHouseException e) {
        super(e);

        this.errorCode = e.getErrorCode();
        this.displayText = extractClickhouseErrorMessage(e);
    }


    public int getErrorCode() {
        return errorCode;
    }

    public String getDisplayText() {
        return displayText;
    }


    private static String extractClickhouseErrorMessage(ClickHouseException e) {
        // try to extract chlickhouse error message

        if (e.getCause() != null) {
            String completeMessage = e.getCause().getMessage();

            //String prefix = "DB::Exception: ";
            String prefix = "e.displayText() = ";
            int leftIndex = completeMessage.indexOf(prefix);

            String suffix = " (version";
            int rightIndex = completeMessage.indexOf(suffix);

            if (leftIndex != -1 && rightIndex > leftIndex)
                return completeMessage.substring(leftIndex + prefix.length(), rightIndex);
        }

        // if exception structure is not known to our logic, return nothing in order not to expose sensitive info
        return null;
    }

}