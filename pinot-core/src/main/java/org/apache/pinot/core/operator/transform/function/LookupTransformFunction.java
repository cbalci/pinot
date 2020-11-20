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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class LookupTransformFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "lookUp";

    // Functions which will evaluate to lookup parameters
    private TransformFunction _dimTableNameFunction;
    private TransformFunction _dimColToLookupFunction;
    private TransformFunction _dimJoinKeyFunction;
    private TransformFunction _factJoinValueFunction;

    // potential result sets
    private String[] _stringValuesSV;

    @Override
    public String getName() {
        return FUNCTION_NAME;
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {

        // Check that there are correct number of arguments
        if (arguments.size() < 4) {
            throw new IllegalArgumentException("At least 4 arguments are required for LOOKUP transform function");
        }

        _dimTableNameFunction = arguments.get(0);
        TransformResultMetadata dimTableNameFunctionResultMetadata = _dimTableNameFunction.getResultMetadata();
        Preconditions.checkState(dimTableNameFunctionResultMetadata.isSingleValue(),
            "First argument must be a string representing dimension table name");
        // TODO validate string
        // TODO validate this dim table exists

        _dimColToLookupFunction = arguments.get(1);
        TransformResultMetadata dimColToLookupFunctionResultMetadata = _dimColToLookupFunction.getResultMetadata();
        Preconditions.checkState(dimColToLookupFunctionResultMetadata.isSingleValue(),
            "Second argument must be a string representing column name do the lookup from");
         // TODO validate this column exists
         // TODO actually lookup the data type of the target column

        _dimJoinKeyFunction = arguments.get(2);
        TransformResultMetadata dimJoinKeyFunctionResultMetadata = _dimJoinKeyFunction.getResultMetadata();
        Preconditions.checkState(dimJoinKeyFunctionResultMetadata.isSingleValue(),
            "Third argument must be the join key on dimension table (primary key)");

        _factJoinValueFunction = arguments.get(3);
//        TransformResultMetadata factJoinValueFunctionResultMetadata = _factJoinValueFunction.getResultMetadata();
//        Preconditions.checkState(!factJoinValueFunctionResultMetadata.isSingleValue(),
//            "Third argument must be the join key on dimension table (primary key)");
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
        return STRING_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
        if (_stringValuesSV == null) {
            _stringValuesSV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        }

        int length = projectionBlock.getNumDocs();
        String[] tableNames = _dimTableNameFunction.transformToStringValuesSV(projectionBlock);
        String[] colNames = _dimColToLookupFunction.transformToStringValuesSV(projectionBlock);
        String[] values = _factJoinValueFunction.transformToStringValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
            _stringValuesSV[i] = lookupDimensionTableColumn(tableNames[i], colNames[i], values[i]);
        }
        return _stringValuesSV;
    }

    private String lookupDimensionTableColumn(String tableName, String columnName, String pk) {
        DimensionTableDataManager mgr = DimensionTableDataManager.getInstanceByTableName(tableName + "_OFFLINE");
        GenericRow row = mgr.lookupRowByPrimaryKey(new PrimaryKey(new String[]{pk}));
        return row.getValue(columnName).toString();
    }
}
