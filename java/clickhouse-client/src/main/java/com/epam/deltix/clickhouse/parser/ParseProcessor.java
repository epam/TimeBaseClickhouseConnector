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
package com.epam.deltix.clickhouse.parser;

import com.epam.deltix.clickhouse.parser.gen.ClickhouseSqlLexer;
import com.epam.deltix.clickhouse.parser.gen.ClickhouseSqlParser;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.IOException;
import java.io.StringReader;

public class ParseProcessor {

    public static SqlDataType parseDataType(final String dataTypeString) {
        if (dataTypeString == null)
            throw new ParseException("Expression can not be null");

        CharStream input = null;
        try {
            input = CharStreams.fromReader(new StringReader(dataTypeString));
        } catch (IOException e) {
        }

        final ClickhouseSqlLexer lexer = new ClickhouseSqlLexer(input);
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final ClickhouseSqlParser parser = new ClickhouseSqlParser(tokens);
        parser.addErrorListener(new ErrorListener());
        final ParseTree tree = parser.root();
        final ParseTreeWalker walker = new ParseTreeWalker();
        final ClickhouseSqlParserImpl visitor = new ClickhouseSqlParserImpl();
        walker.walk(visitor, tree);
        if (visitor.getNumberOfErrors() != 0)
            throw new IllegalStateException(String.format("Cannot parse data type string '%s'.", dataTypeString));

        return visitor.getDataType();
    }

}