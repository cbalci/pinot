/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class LookupTransformFunctionTest extends BaseTransformFunctionTest {
    private static final String TABLE_NAME = "baseballTeams_OFFLINE";
    private DimensionTableDataManager tableManager;

    @BeforeSuite
    public void setUp() throws Exception {
        super.setUp();

        tableManager = mock(DimensionTableDataManager.class);
        DimensionTableDataManager.registerDimensionTable(TABLE_NAME, tableManager);

        when(tableManager.getColumnFieldSpec("teamName")).thenReturn(
                new DimensionFieldSpec("teamName", FieldSpec.DataType.STRING, true)
        );
        when(tableManager.getColumnFieldSpec("teamID")).thenReturn(
                new DimensionFieldSpec("teamID", FieldSpec.DataType.STRING, true)
        );
    }

    @Test
    public void instantiationTests() throws Exception {
        // Success case
        ExpressionContext expression = QueryContextConverterUtils.getExpression(String
                .format("lookup('baseballTeams','teamName','teamID',%s)", STRING_SV_COLUMN));
        TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
        Assert.assertTrue(transformFunction instanceof LookupTransformFunction);
        Assert.assertEquals(transformFunction.getName(), LookupTransformFunction.FUNCTION_NAME);

        // Wrong number of arguments
        Assert.assertThrows(BadQueryRequestException.class, () -> {
            TransformFunctionFactory.get(QueryContextConverterUtils.getExpression(String
                    .format("lookup('baseballTeams','teamName','teamID')")), _dataSourceMap);
        });

        // Wrong number of join keys
        Assert.assertThrows(BadQueryRequestException.class, () -> {
            TransformFunctionFactory.get(QueryContextConverterUtils.getExpression(String
                    .format("lookup('baseballTeams','teamName','teamID', %s, 'danglingKey')", STRING_SV_COLUMN)),
                    _dataSourceMap);
        });

        // Non literal tableName argument
        Assert.assertThrows(BadQueryRequestException.class, () -> {
            TransformFunctionFactory.get(QueryContextConverterUtils.getExpression(String
                            .format("lookup(%s,'teamName','teamID', %s)", STRING_SV_COLUMN, INT_SV_COLUMN)),
                    _dataSourceMap);
        });

        // Non literal lookup columnName argument
        Assert.assertThrows(BadQueryRequestException.class, () -> {
            TransformFunctionFactory.get(QueryContextConverterUtils.getExpression(String
                            .format("lookup('baseballTeams',%s,'teamID',%s)", STRING_SV_COLUMN, INT_SV_COLUMN)),
                    _dataSourceMap);
        });

        // Non literal lookup columnName argument
        Assert.assertThrows(BadQueryRequestException.class, () -> {
            TransformFunctionFactory.get(QueryContextConverterUtils.getExpression(String
                            .format("lookup('baseballTeams','teamName',%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN)),
                    _dataSourceMap);
        });
    }

    // TODO More coverage
}
