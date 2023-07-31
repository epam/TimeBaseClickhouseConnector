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
package com.epam.deltix.timebase.connector.clickhouse.util;

import java.util.function.Function;

public class StringUtil {

    /**
     * Replace invalid characters with `_` + `char code` of the invalid character.
     */
    public static StringBuilder replaceWithCharCode(String str, String validPatter, int start, int end){
        Function<Character, String> replacementWithCharCode = (character) -> String.format("_%d", (int) character);
        return replaceInvalidCharacters(str, validPatter, replacementWithCharCode, start, end);
    }

    /**
     * Replace invalid characters using `replacementRule`.
     *
     * @param   str                 the String to search for.
     * @param   validPattern        valid symbols in the regular expression notation.
     * @param   replacementRule     function according to which an invalid character is replaced with a given string.
     * @param   start               replacement start index.
     * @param   end                 replacement end index.
     *
     * @return  a {{@link StringBuilder}} without invalid characters.
     */
    public static StringBuilder replaceInvalidCharacters(String str, String validPattern, Function<Character, String> replacementRule, int start, int end) {
        if (start < 0)
            start = 0;
        if (end < 0)
            end = str.length();

        StringBuilder output = new StringBuilder(end - start);
        for (int i = start; i < end; i++)
        {
            char c = str.charAt(i);

            if (String.valueOf(c).matches(validPattern))
                output.append(c);
            else
                output.append(replacementRule.apply(c));
        }

        return output;
    }
}