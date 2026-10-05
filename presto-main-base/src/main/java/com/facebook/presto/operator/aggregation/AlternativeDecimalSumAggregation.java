/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.google.common.collect.MoreCollectors.onlyElement;

/** Coordinator metadata for cuDF Decimal64 SUM; no Java execution implementation. */
public class AlternativeDecimalSumAggregation
        extends DecimalSumAggregation
{
    public static final AlternativeDecimalSumAggregation ALTERNATIVE_DECIMAL_SUM_AGGREGATION = new AlternativeDecimalSumAggregation();

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type inputType = applyBoundVariables(functionAndTypeManager, getSignature().getArgumentTypes(), boundVariables)
                .stream()
                .collect(onlyElement());
        Type outputType = applyBoundVariables(functionAndTypeManager, getSignature().getReturnType(), boundVariables);
        if (!(inputType instanceof DecimalType) || !((DecimalType) inputType).isShort()) {
            return super.specialize(boundVariables, arity, functionAndTypeManager);
        }
        return new BuiltInAggregationFunctionImplementation(
                "sum", ImmutableList.of(inputType), ImmutableList.of(outputType), outputType,
                true, false, null, null, null);
    }
}
