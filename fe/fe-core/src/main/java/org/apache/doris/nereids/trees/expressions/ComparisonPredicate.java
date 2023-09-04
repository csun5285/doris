// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import java.util.List;

/**
 * Comparison predicate expression.
 * Such as: "=", "<", "<=", ">", ">=", "<=>"
 */
public abstract class ComparisonPredicate extends BinaryOperator {

    public ComparisonPredicate(List<Expression> children, String symbol) {
        super(children, symbol);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitComparisonPredicate(this, context);
    }

    @Override
    public AbstractDataType inputType() {
        return AnyDataType.INSTANCE;
    }

    /**
     * Commute between left and right children.
     */
    public abstract ComparisonPredicate commute();

    @Override
    public TypeCheckResult checkInputDataTypes() {
        for (int i = 0; i < super.children().size(); i++) {
            Expression input = super.children().get(i);
            if (input.getDataType().isObjectType()) {
                return new TypeCheckResult(false,
                    "Bitmap type does not support operator:" + this.toSql());
            }
        }
        return TypeCheckResult.SUCCESS;
    }
}
