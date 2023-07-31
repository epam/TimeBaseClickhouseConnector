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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;

@Testcontainers
public class DockerTimebaseContainer {
    private static final Log LOG = LogFactory.getLog(DockerTimebaseContainer.class);
    private static final String DOCKER_IMAGE_NAME = "finos/timebase-ce-server:6.1.14";
    private static final String TIMEBASE_SERIAL_NUMBER_SYSTEM_VAR_NAME = "TIMEBASE_SERIAL_NUMBER";
    private static volatile DockerTimebaseContainer instance;
    @Container
    private static GenericContainer<?> container;

    private DockerTimebaseContainer() {
        LOG.info("Initialization...");
        container = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withExposedPorts(8011)
                .withCopyFileToContainer(attachLicense(), "/timebase-server/inst.properties");

        LOG.info().append("Container parameters: ").append(container.toString()).commit();
        LOG.info("Container starting..");
        container.start();
        LOG.info("Container successfully started.");
    }

    private static MountableFile attachLicense() {
        try {
            LOG.info("Start license file creation.");
            Path license = Files.createTempFile("inst", ".properties");

            final String serialNumber = System.getenv(TIMEBASE_SERIAL_NUMBER_SYSTEM_VAR_NAME);
            final String fileContent = String.format("serial=%s", serialNumber);

            LOG.info("Start writing to the temp system file.");
            Files.write(license, fileContent.getBytes());
            LOG.info().append("License file created successfully. ").append("Location: ").append(license.toAbsolutePath().toString()).commit();

            return MountableFile.forHostPath(license);
        } catch (Exception ex) {
            LOG.info().append("Exception during creating temp system file. ").append(ex).commit();
            throw new IllegalArgumentException(ex);
        }
    }

    public static DockerTimebaseContainer getInstance() {
        if (instance == null) {
            synchronized (DockerTimebaseContainer.class) {
                if (instance == null)
                    instance = new DockerTimebaseContainer();
            }
        }
        return instance;
    }

    public GenericContainer<?> getContainer() {
        return container;
    }
}