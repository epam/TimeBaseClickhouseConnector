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
package com.epam.deltix.clickhouse.selector;


import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.selector.params.SelectParam;
import com.epam.deltix.clickhouse.selector.params.UInt64Param;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class SelectBuilder extends SubqueryNode implements WrappableNode {


    public static class SubqueryRawNode extends SubqueryNode {

        private final String expression;

        public SubqueryRawNode(String expression) {
            this.expression = expression;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            sb.append(expression);
        }
    }

    public static class JoinNode extends SubqueryNode {

        private final SubqueryNode left;
        private final SubqueryNode right;
        private final JoinType joinType;
        private final List<String> onExpressions;
        private final List<String> usingColumns;

        public JoinNode(SubqueryNode left, SubqueryNode right, JoinType joinType, List<String> onExpressions, List<String> usingColumns) {

            if (left == null)
                throw new IllegalArgumentException("left");

            if (right == null)
                throw new IllegalArgumentException("right");

            if (onExpressions == null && usingColumns == null)
                throw new IllegalArgumentException("ON and USING cannot be simultaneously null");

            if (onExpressions != null && usingColumns != null)
                throw new IllegalArgumentException("ON and USING cannot be simultaneously not null");

            this.left = left;
            this.right = right;
            this.joinType = joinType;
            this.onExpressions = onExpressions;
            this.usingColumns = usingColumns;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {

            left.append(sb, params);
            sb.append(" ALL ");

            switch (joinType) {
                case INNER:
                    sb.append("INNER JOIN");
                    break;
                case LEFT_OUTER:
                    sb.append("LEFT OUTER JOIN");
                    break;
                case RIGHT_OUTER:
                    sb.append("RIGHT OUTER JOIN");
                    break;
                case FULL_OUTER:
                    sb.append("FULL OUTER JOIN");
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Join type %s is not supported.", joinType));
            }

            sb.append(" (");
            right.append(sb, params);
            sb.append(")");

            if (onExpressions != null) {
                sb.append(" ON ");

                for (int i = 0; i < onExpressions.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }

                    sb.append(onExpressions.get(i));
                }
            }

            if (usingColumns != null) {
                sb.append(" USING ");

                for (int i = 0; i < usingColumns.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }

                    sb.append(usingColumns.get(i));
                }
            }
        }
    }

    public static class TableIdentityNode extends SubqueryNode {
        private final TableIdentity tableIdentity;

        public TableIdentityNode(TableIdentity tableIdentity) {
            this.tableIdentity = tableIdentity;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            sb.append(tableIdentity.toString());
        }
    }

    public static class FromBuilder extends SubqueryNode {

        private final SelectBuilder selectBuilder;
        private SubqueryNode from;

        private final List<String> arrayJoin = new ArrayList<>();

        public FromBuilder(SelectBuilder selectBuilder) {
            this.selectBuilder = selectBuilder;
        }

        public FromBuilder from(String from) {
            this.from = new SubqueryRawNode(from);
            return this;
        }

        public FromBuilder from(TableIdentity from) {
            this.from = new TableIdentityNode(from);
            return this;
        }

        public FromBuilder from(SubqueryNode from) {
            this.from = from;
            return this;
        }

        public FromBuilder innerJoinOn(String right, String... onExpressions) {
            return innerJoinOn(new SubqueryRawNode(right), onExpressions);
        }

        public FromBuilder innerJoinOn(SubqueryNode right, String... onExpressions) {
            return joinOn(JoinType.INNER, right, onExpressions);
        }

        public FromBuilder leftJoinOn(String right, String... onExpressions) {
            return leftJoinOn(new SubqueryRawNode(right), onExpressions);
        }

        public FromBuilder leftJoinOn(SubqueryNode right, String... onExpressions) {
            return joinOn(JoinType.LEFT_OUTER, right, onExpressions);
        }

        public FromBuilder rightJoinOn(String right, String... onExpressions) {
            return rightJoinOn(new SubqueryRawNode(right), onExpressions);
        }

        public FromBuilder rightJoinOn(SubqueryNode right, String... onExpressions) {
            return joinOn(JoinType.RIGHT_OUTER, right, onExpressions);
        }

        public FromBuilder fullJoinOn(String right, String... onExpressions) {
            return fullJoinOn(new SubqueryRawNode(right), onExpressions);
        }

        public FromBuilder fullJoinOn(SubqueryNode right, String... onExpressions) {
            return joinOn(JoinType.FULL_OUTER, right, onExpressions);
        }

        public FromBuilder joinOn(JoinType joinType, String right, String... onExpressions) {
            return joinOn(joinType, new SubqueryRawNode(right), onExpressions);
        }

        public FromBuilder joinOn(JoinType joinType, SubqueryNode right, String... onExpressions) {
            this.from = new JoinNode(from, right, joinType, Arrays.asList(onExpressions), null);
            return this;
        }


        public FromBuilder innerJoinUsing(String right, String... usingColumns) {
            return innerJoinUsing(new SubqueryRawNode(right), usingColumns);
        }

        public FromBuilder innerJoinUsing(SubqueryNode right, String... usingColumns) {
            return joinUsing(JoinType.INNER, right, usingColumns);
        }

        public FromBuilder leftJoinUsing(String right, String... usingColumns) {
            return leftJoinUsing(new SubqueryRawNode(right), usingColumns);
        }

        public FromBuilder leftJoinUsing(SubqueryNode right, String... usingColumns) {
            return joinUsing(JoinType.LEFT_OUTER, right, usingColumns);
        }

        public FromBuilder rightJoinUsing(String right, String... usingColumns) {
            return rightJoinUsing(new SubqueryRawNode(right), usingColumns);
        }

        public FromBuilder rightJoinUsing(SubqueryNode right, String... usingColumns) {
            return joinUsing(JoinType.RIGHT_OUTER, right, usingColumns);
        }

        public FromBuilder fullJoinUsing(String right, String... usingColumns) {
            return fullJoinUsing(new SubqueryRawNode(right), usingColumns);
        }

        public FromBuilder fullJoinUsing(SubqueryNode right, String... usingColumns) {
            return joinUsing(JoinType.FULL_OUTER, right, usingColumns);
        }

        public FromBuilder joinUsing(JoinType joinType, String right, String... usingColumns) {
            return joinUsing(joinType, new SubqueryRawNode(right), usingColumns);
        }

        public FromBuilder joinUsing(JoinType joinType, SubqueryNode right, String... usingColumns) {
            this.from = new JoinNode(from, right, joinType, null, Arrays.asList(usingColumns));
            return this;
        }

        public FromBuilder arrayJoin(String expression) {
            // TODO: re-write to SubqueryNode abstraction
            arrayJoin.add(expression);
            return this;
        }

        public SelectBuilder done() {
            return selectBuilder;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            sb.append("FROM ");

            if (from instanceof WrappableNode) {
                sb.append("(");
                from.append(sb, params);
                sb.append(")");
            } else {
                from.append(sb, params);
            }

            for (int i = 0; i < arrayJoin.size(); ++i) {
                if (i == 0)
                    sb.append(" ARRAY JOIN ");
                else
                    sb.append(", ");

                sb.append(arrayJoin.get(i));
            }
        }
    }

    public static class WhereBuilder extends ElementBuilder {

        private final SelectBuilder selectBuilder;
        private WhereNode where;

        public WhereBuilder(SelectBuilder selectBuilder) {
            this.selectBuilder = selectBuilder;
        }

        public WhereBuilder where(WhereNode where) {
            this.where = where;
            return this;
        }

        public WhereBuilder where(String filterExpression, String operator, SelectParam... params) {
            where = new WhereExpression(filterExpression, operator, params);
            return this;
        }

        public WhereBuilder where(String rawExpression, SelectParam... params) {
            where = new WhereRawExpression(rawExpression, params);
            return this;
        }

        public WhereBuilder and(String filterExpression, String operator, SelectParam... params) {
            return and(new WhereExpression(filterExpression, operator, params));
        }

        public WhereBuilder and(String rawExpression, SelectParam... params) {
            return and(new WhereRawExpression(rawExpression, params));
        }

        public WhereBuilder and(WhereSingleExpression newExpression) {
            return complexOperator(WhereOperator.AND, newExpression);
        }

        public WhereBuilder or(String filterExpression, String operator, SelectParam... params) {
            return or(new WhereExpression(filterExpression, operator, params));
        }

        public WhereBuilder or(String rawExpression, SelectParam... params) {
            return or(new WhereRawExpression(rawExpression, params));
        }

        public WhereBuilder or(WhereSingleExpression newExpression) {
            return complexOperator(WhereOperator.OR, newExpression);
        }

        private WhereBuilder complexOperator(WhereOperator operator, WhereSingleExpression newExpression) {
            if (where == null)
                throw new IllegalStateException("where is null");

            if (where instanceof WhereMultipleExpression && ((WhereMultipleExpression)where).operator == operator)
                ((WhereMultipleExpression)where).expressions.add(newExpression);
            else
                where = new WhereMultipleExpression(operator, where, newExpression);

            return this;
        }

        public SelectBuilder done() {
            return selectBuilder;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            if (where == null)
                return;

            sb.append("WHERE ");
            where.append(sb, params);
        }
    }

    public static abstract class WhereNode extends ElementBuilder {

    }

    public static class WhereMultipleExpression extends WhereNode {
        private WhereOperator operator;
        private List<WhereNode> expressions;


        public WhereMultipleExpression(WhereOperator operator, WhereNode... expressions) {
            this.operator = operator;
            this.expressions = Arrays.stream(expressions).collect(Collectors.toList());
        }


        public List<WhereNode> getExpressions() {
            return expressions;
        }

        public void setExpressions(List<WhereNode> expressions) {
            this.expressions = expressions;
        }

        public WhereOperator getOperator() {
            return operator;
        }

        public void setOperator(WhereOperator operator) {
            this.operator = operator;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            for (int i = 0; i < expressions.size(); i++) {
                if (i > 0) {
                    sb.append(' ');
                    sb.append(operator);
                    sb.append(' ');
                }

                WhereNode whereExpression = expressions.get(i);
                sb.append('(');
                whereExpression.append(sb, params);
                sb.append(')');
            }
        }
    }

    public static abstract class WhereSingleExpression extends WhereNode {
        private String expression;
        private SelectParam[] params;


        public WhereSingleExpression(String expression, SelectParam[] params) {
            this.expression = expression;
            this.params = params;
        }

        public String getExpression() {
            return expression;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }

        public SelectParam[] getParams() {
            return params;
        }

        public void setParams(SelectParam[] params) {
            this.params = params;
        }
    }

    public static class WhereExpression extends WhereSingleExpression {
        private String operator;

        public WhereExpression(String expression, String operator, SelectParam[] params) {
            super(expression, params);

            this.operator = operator;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }


        @Override
        protected void append(StringBuilder sb, List<SelectParam> queryParams) {
            SelectParam[] params = this.getParams();

            if (this.operator.equals("IN") || this.operator.equals("NOT IN")) {
                if (params.length < 1)
                    throw illegalFilterValue(this);

                sb.append(getExpression());
                sb.append(" ");
                sb.append(getOperator());
                sb.append(" (");

                for (int i = 0; i < params.length ; ++i) {
                    if (i > 0)
                        sb.append(", ");

                    sb.append("?");
                    queryParams.add(params[i]);
                }
                sb.append(")");
            } else {
                if (params.length != 1)
                    throw illegalFilterValue(this);

                sb.append(getExpression());
                sb.append(" ");
                sb.append(getOperator());
                sb.append(" ?");
                queryParams.add(params[0]) ;
            }
        }
    }

    public static class WhereSubqueryExpression extends WhereNode {
        private String expression;
        private String operator;
        private ElementBuilder subquery;

        public WhereSubqueryExpression(String expression, String operator, ElementBuilder subquery) {
            this.expression = expression;
            this.operator = operator;
            this.subquery = subquery;
        }

        public String getExpression() {
            return expression;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }

        public ElementBuilder getSubquery() {
            return subquery;
        }

        public void setSubquery(ElementBuilder subquery) {
            this.subquery = subquery;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            sb.append(getExpression());
            sb.append(" ");
            sb.append(getOperator());
            sb.append(" (");
            getSubquery().append(sb, params);
            sb.append(")");
        }
    }

    public static class WhereRawExpression extends WhereSingleExpression {

        public WhereRawExpression(String expression, SelectParam[] params) {
            super(expression, params);
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> queryParams) {
            sb.append(getExpression());
            SelectParam[] params = this.getParams();

            for (SelectParam param : params)
                queryParams.add(param);
        }
    }

    private class GroupByExpression extends ElementBuilder {
        private String expression;

        public GroupByExpression(String expression) {
            this.expression = expression;
        }

        public String getExpression() {
            return expression;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            sb.append(expression);
        }
    }

    private class OrderByExpression extends ElementBuilder {
        private String expression;
        private String direction;

        public OrderByExpression(String expression, String direction) {
            this.expression = expression;
            this.direction = direction;
        }

        public String getExpression() {
            return expression;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }

        public String getDirection() {
            return direction;
        }

        public void setDirection(String direction) {
            this.direction = direction;
        }

        @Override
        protected void append(StringBuilder sb, List<SelectParam> params) {
            sb.append(expression);

            if (direction != null) {
                sb.append(" ");
                sb.append(direction);
            }
        }
    }

    public static final String ASC = "ASC";
    public static final String DESC = "DESC";

    private static final String GROUP_BY = "GROUP BY";
    private static final String ORDER_BY = "ORDER BY";

    private final FromBuilder fromBuilder = new FromBuilder(this);
    private final List<String> expressions = new ArrayList<>();
    private final WhereBuilder whereBuilder = new WhereBuilder(this);
    private final List<ElementBuilder> groupBy = new ArrayList<>();
    private final List<ElementBuilder> orderBy = new ArrayList<>();
    private long skip = Long.MIN_VALUE;
    private long take = Long.MIN_VALUE;
    private boolean distinct = false;

    private List<SelectParam> params = new ArrayList<>();

    public FromBuilder from(String from) {
        return fromBuilder.from(from);
    }

    public FromBuilder from(TableIdentity from) {
        return fromBuilder.from(from);
    }

    public FromBuilder from(SubqueryNode from) {
        return fromBuilder.from(from);
    }

    public SelectBuilder expressions(Collection<String> expressions) {
        this.expressions.addAll(expressions);
        return this;
    }

    public SelectBuilder expressions(String... expressions) {
        for (String expression : expressions)
            this.expressions.add(expression);
        return this;
    }

    public WhereBuilder where(WhereNode where) {
        return whereBuilder.where(where);
    }

    public WhereBuilder where(String filterExpression, String operator, SelectParam... params) {
        return whereBuilder.where(filterExpression, operator, params);
    }

    public WhereBuilder where(String rawExpression, SelectParam... params) {
        return whereBuilder.where(rawExpression, params);
    }

    public SelectBuilder groupBy(Collection<String> expressions) {
        groupBy.addAll(expressions.stream().map(GroupByExpression::new).collect(Collectors.toList()));
        return this;
    }

    public SelectBuilder groupBy(String expression) {
        groupBy.add(new GroupByExpression(expression));
        return this;
    }

    public SelectBuilder orderBy(Collection<String> expressions) {
        return orderBy(expressions, null);
    }

    public SelectBuilder orderBy(Collection<String> expressions, String direction) {
        orderBy.addAll(expressions.stream().map(item -> new OrderByExpression(item, direction)).collect(Collectors.toList()));
        return this;
    }

    public SelectBuilder orderBy(String expression) {
        return orderBy(expression, null);
    }

    public SelectBuilder orderBy(String expression, String direction) {
        orderBy.add(new OrderByExpression(expression, direction));
        return this;
    }

    public SelectBuilder limit(long skip, long take) {
        this.skip = skip;
        this.take = take;
        return this;
    }

    public SelectBuilder limit(long take) {
        this.take = take;
        return this;
    }

    public SelectBuilder distinct() {
        this.distinct = true;
        return this;
    }

    public List<SelectParam> getParams() {
        return params;
    }

    public String build() {
        StringBuilder sb = new StringBuilder();
        append(sb, params);
        return sb.toString();
    }

    @Override
    protected void append(StringBuilder sb, List<SelectParam> params) {
        sb.append("SELECT ");

        if (distinct)
            sb.append("DISTINCT ");

        for (int i = 0; i < expressions.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append(expressions.get(i));
        }

        sb.append(" ");
        fromBuilder.append(sb, params);
        sb.append(' ');
        whereBuilder.append(sb, params);

        appendElements(sb, params, groupBy, GROUP_BY);
        appendElements(sb, params, orderBy, ORDER_BY);

        if (take != Long.MIN_VALUE) {
            if (skip != Long.MIN_VALUE) {
                sb.append(" LIMIT ?, ?");
                params.add(new UInt64Param(skip));
                params.add(new UInt64Param(take));
            } else {
                sb.append(" LIMIT ?");
                params.add(new UInt64Param(take));
            }
        }
    }

    private void appendElements(StringBuilder sb, List<SelectParam> params,
                                List<ElementBuilder> elements, String function) {
        for (int i = 0; i < elements.size(); ++i) {
            if (i == 0) {
                sb.append(" ");
                sb.append(function);
                sb.append(" ");
            } else {
                sb.append(", ");
            }

            elements.get(i).append(sb, params);
        }
    }

    private static RuntimeException illegalFilterValue(WhereExpression whereExpr) {
        throw new IllegalArgumentException(String.format("Invalid value for expression '%s' with operator '%s'. Actual size - %d",
            whereExpr.getExpression(), whereExpr.getOperator(), whereExpr.getParams().length));
    }
}