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
package com.facebook.presto.sql.planner.optimizations.calcite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoOutput;
import com.facebook.presto.sql.planner.plan.calcite.PrestoRelNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoTableScan;
import com.facebook.presto.sql.planner.plan.calcite.RelOptPrestoTable;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;

import static com.google.common.collect.Iterables.getOnlyElement;

public class CalciteConverter
        extends PlanVisitor<CalciteConverter.Context, RelNode>
{

    @Override
    protected PrestoRelNode visitPlan(PlanNode node, Context context)
    {
        throw new UnsupportedOperationException("Presto -> Calcite conversion for " + node.getClass().getSimpleName() + " not yet implemented");
    }

    @Override
    public PrestoRelNode visitOutput(OutputNode node, Context context)
    {
        RelNode child = visitChild(node, context);
        return new PrestoOutput(context.cluster, getTraitSet(context), child, node.getColumnNames(), node.getOutputSymbols());
    }

    private RelTraitSet getTraitSet(Context context)
    {
        return context.cluster.traitSetOf(PrestoRelNode.CONVENTION);
    }

    private RelNode visitChild(PlanNode node, Context context)
    {
        return getOnlyElement(node.getSources()).accept(this, context);
    }

    @Override
    public PrestoRelNode visitTableScan(TableScanNode node, Context context)
    {
        RelOptPrestoTable prestoTable = new RelOptPrestoTable(
                context.relOptSchema,
                getTableName(node, context),
                context.cluster.getTypeFactory().createJavaType(int.class),
                node.getTable(),
                ImmutableList.copyOf(node.getAssignments().values()));
        PrestoTableScan scan = new PrestoTableScan(context.cluster, getTraitSet(context), prestoTable);
        return scan;
    }

    private String getTableName(TableScanNode node, Context context)
    {
        return context.metadata.getTableMetadata(context.session, node.getTable()).getTable().getTableName();
    }

    public static class Context
    {
        private final RelOptCluster cluster;
        private final RelOptSchema relOptSchema;
        private final SchemaPlus rootSchema;
        private final Metadata metadata;
        private final Session session;

        public Context(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema, Metadata metadata, Session session)
        {
            this.cluster = cluster;
            this.relOptSchema = relOptSchema;
            this.rootSchema = rootSchema;
            this.metadata = metadata;
            this.session = session;
        }
    }
}
