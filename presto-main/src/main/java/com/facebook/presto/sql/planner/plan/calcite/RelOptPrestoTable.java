/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan.calcite;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class RelOptPrestoTable
        extends RelOptAbstractTable
{
    private final TableHandle table;
    private final List<ColumnHandle> assignments;

    public RelOptPrestoTable(RelOptSchema schema, String tableName, RelDataType dataType, TableHandle table,
            List<ColumnHandle> assignments)
    {
        super(schema, tableName, dataType);
        this.table = table;
        this.assignments = assignments;
    }

    @Override
    public double getRowCount()
    {
        if (getName().contains("nation")) {
            return 25;
        }
        else if (getName().contains("region")) {
            return 5;
        }
        return super.getRowCount();
    }

    public TableHandle getTable()
    {
        return table;
    }

    public List<ColumnHandle> getAssignments()
    {
        return assignments;
    }
}
