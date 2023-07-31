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
package com.epam.deltix.clickhouse.util;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorsUtil {
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 60_000;
    private static final Log LOG = LogFactory.getLog(ExecutorsUtil.class);

    public static void shutdownAndAwaitTermination(ExecutorService pool, long timeoutMs) {
        String description = String.valueOf(pool);


        LOG.debug()
                .append("Shutting down executor service @")
                .append(description)
                .append(". Awaiting termination of scheduled tasks.")
                .commit();

        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                LOG.warn()
                        .append("Scheduled tasks for executor service @")
                        .append(description)
                        .append(" failed to complete in ")
                        .append(timeoutMs)
                        .append(" ms. Aborting unfinished tasks.")
                        .commit();

                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                    LOG.error()
                            .append("Scheduled tasks for executor service @")
                            .append(description)
                            .append(" failed to interrupt in ")
                            .append(timeoutMs)
                            .append(" ms.")
                            .commit();
                }
            } else {
                LOG.debug()
                        .append("Successfully terminated executor service @")
                        .append(description)
                        .commit();
            }
        } catch (InterruptedException ie) {
            LOG.warn()
                    .append("Awaiting thread for executor service @")
                    .append(description)
                    .append(" was interrupted. Trying to re-cancel unfinished tasks.")
                    .commit();

            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}