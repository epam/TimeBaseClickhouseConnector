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
package com.epam.deltix.timebase.connector.clickhouse.model;

public class StreamRequest extends ReplicationRequest {

    private String stream;

    public StreamRequest() {
    }

    /**
     * Source stream key     
     */
    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
        if (getTable() == null) {
            setTable(stream);
        }
        if (getKey() == null) {
            setKey(stream);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        StreamRequest that = (StreamRequest) o;

        return stream.equals(that.stream);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + stream.hashCode();
        return result;
    }
}