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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'random'. This class is generated by GenerateFunction.
 */
public class Random extends ScalarFunction
        implements ExplicitlyCastableSignature, Nondeterministic {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE)
    );

    /**
     * constructor with 0 argument.
     */
    public Random() {
        super("random");
    }

    /**
     * constructor with 1 argument.
     */
    public Random(Expression arg) {
        super("random", arg);
        // align with original planner behavior, refer to: org/apache/doris/analysis/Expr.getBuiltinFunction()
        Preconditions.checkState(arg instanceof Literal, "The param of rand function must be literal");
    }

    /**
     * custom compute nullable.
     */
    @Override
    public boolean nullable() {
        if (arity() > 0) {
            return children().stream().anyMatch(Expression::nullable);
        } else {
            return false;
        }
    }

    /**
     * withChildren.
     */
    @Override
    public Random withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 0
                || children.size() == 1);
        if (children.isEmpty() && arity() == 0) {
            return this;
        } else {
            return new Random(children.get(0));
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRandom(this, context);
    }
}
