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

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

public class ReflectionUtil {
    private static final Log LOG = LogFactory.getLog(ReflectionUtil.class);

    public static PropertyDescriptor getPropertyByGetMethod(Method method) {
        try {
            Class<?> clazz = method.getDeclaringClass();
            BeanInfo info = Introspector.getBeanInfo(clazz);
            PropertyDescriptor[] props = info.getPropertyDescriptors();
            for (PropertyDescriptor pd : props) {
                if (method.equals(pd.getReadMethod()))
                    return pd;
            }
        } catch (Exception e) {
            LOG.error()
                    .append("Error while getting field name for ")
                    .append(method.getName())
                    .append(e)
                    .commit();
        }

        return null;
    }
}