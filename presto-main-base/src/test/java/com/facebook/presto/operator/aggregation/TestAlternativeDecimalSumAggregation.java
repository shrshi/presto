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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAlternativeDecimalSumAggregation
{
    @Test
    public void testCudfRegistration()
    {
        // Ordinary native execution must keep its existing decimal state.
        for (boolean alternativeSignatures : new boolean[] {false, true}) {
            for (boolean cudfEnabled : new boolean[] {false, true}) {
                FunctionAndTypeManager functions = createTestMetadataManager(new FunctionsConfig()
                        .setUseAlternativeFunctionSignatures(alternativeSignatures)
                        .setCudfEnabled(cudfEnabled)).getFunctionAndTypeManager();
                for (int precision : new int[] {1, 18, 19, 38}) {
                    for (int scale : new int[] {0, precision}) {
                        DecimalType input = createDecimalType(precision, scale);
                        BuiltInAggregationFunctionImplementation sum = (BuiltInAggregationFunctionImplementation)
                                functions.getAggregateFunctionImplementation(functions.lookupFunction("sum", fromTypes(input)));
                        Type output = createDecimalType(38, scale);
                        boolean typed = cudfEnabled && input.isShort();
                        assertEquals(sum.getIntermediateType(), typed ? output : VARBINARY);
                        assertEquals(sum.getFinalType(), output);
                        assertTrue(sum.isDecomposable());
                        if (typed) {
                            assertNull(sum.getAggregationMetadata());
                            assertNull(sum.getAccumulatorClass());
                            assertNull(sum.getGroupedAccumulatorClass());
                        }
                        assertEquals(functions.getAggregateFunctionImplementation(
                                functions.lookupFunction("avg", fromTypes(input))).getIntermediateType(), VARBINARY);
                    }
                }
                assertEquals(functions.getAggregateFunctionImplementation(
                        functions.lookupFunction("sum", fromTypes(BIGINT))).getIntermediateType(), BIGINT);
            }
        }
    }
}
