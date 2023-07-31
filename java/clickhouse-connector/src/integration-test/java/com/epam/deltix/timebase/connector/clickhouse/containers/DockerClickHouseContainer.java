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
package com.epam.deltix.timebase.connector.clickhouse.containers;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class DockerClickHouseContainer {
    private static final Log LOG = LogFactory.getLog(DockerClickHouseContainer.class);

    private static volatile DockerClickHouseContainer instance;

    @Container
    private static ClickHouseContainer container;
    private static final String BASE_DOCKER_IMAGE_NAME = "clickhouse/clickhouse-server";

    private DockerClickHouseContainer(String version) {
        LOG.info("Initialization...");

        version = StringUtils.isEmpty(version) ? "" : ":" + version;
        container = new ClickHouseContainer(BASE_DOCKER_IMAGE_NAME + version);

        LOG.info().append("Container parameters: ").append(container.toString()).commit();
        LOG.info("Container starting..");

        container.start();

        LOG.info("Container successfully started.");
    }

    public static DockerClickHouseContainer getInstance(String version) {
        if (instance == null) {
            synchronized (DockerClickHouseContainer.class) {
                if (instance == null)
                    instance = new DockerClickHouseContainer(version);
            }
        }
        return instance;
    }

    public ClickHouseContainer getContainer() {
        return container;
    }
}