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
package com.epam.deltix.timebase.connector.clickhouse.configuration.beans;

import com.epam.deltix.qsrv.hf.spi.conn.Disconnectable;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickDB;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickDBFactory;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.TimebaseProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class TimeBaseConnectorService {
    private final DXTickDB tickDB;

    @Autowired
    public TimeBaseConnectorService(TimebaseProperties settings) {
        TickDBFactory.setApplicationName("axa-backend");
        if (settings.getUsername() != null && !settings.getUsername().isEmpty())
            tickDB = TickDBFactory.createFromUrl(settings.getUrl(), settings.getUsername(), settings.getPassword());
        else
            tickDB = TickDBFactory.createFromUrl(settings.getUrl());
   }

    @PreDestroy
    private void destroy() {
        tickDB.close();
    }

    @Bean
    public DXTickDB getTickDB() {
        return tickDB;
    }

    public boolean isConnected() {
        if (tickDB instanceof Disconnectable)
            return ((Disconnectable) tickDB).isConnected();

        // failover in case DXTickDB does not implement
        return true;
    }
}