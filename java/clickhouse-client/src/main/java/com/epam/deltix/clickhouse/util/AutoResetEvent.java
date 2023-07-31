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

public class AutoResetEvent
{
    private final Object monitor = new Object();
    private volatile boolean isOpen = false;

    public AutoResetEvent(boolean open)
    {
        isOpen = open;
    }

    public void waitOne() throws InterruptedException
    {
        synchronized (monitor) {
            while (!isOpen) {
                monitor.wait();
            }
            isOpen = false;
        }
    }

    public boolean waitOne(long timeout) throws InterruptedException {
        synchronized (monitor) {
            try {
                long t = System.currentTimeMillis();
                while (!isOpen) {
                    monitor.wait(timeout);
                    // Check for timeout
                    if (System.currentTimeMillis() - t >= timeout)
                        break;
                }

                return isOpen;
            }
            finally {
                isOpen = false;
            }
        }
    }
    public void set()
    {
        synchronized (monitor) {
            isOpen = true;
            monitor.notify();
        }
    }

    public void reset()
    {
        isOpen = false;
    }
}