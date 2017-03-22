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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.iterative.MemoBasedLookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertEquals;

public class TestMemoBasedLookup
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder().build());
    }

    @Test
    public void testResolvesGroupReferenceNode()
    {
        PlanNode source = node();
        PlanNode plan = node(source);
        Memo memo = new Memo(idAllocator, plan);

        MemoBasedLookup lookup = new MemoBasedLookup(memo, new NodeCountingCostCalculator());
        PlanNode memoSource = Iterables.getOnlyElement(memo.getNode(memo.getRootGroup()).getSources());
        checkState(memoSource instanceof GroupReference, "expected GroupReference");
        assertEquals(lookup.resolve(memoSource), source);
    }

    @Test
    public void testComputesCostAndResolvesNodes()
    {
        PlanNode plan = node(node(node()));
        Memo memo = new Memo(idAllocator, plan);
        MemoBasedLookup lookup = new MemoBasedLookup(memo, new NodeCountingCostCalculator());

        PlanNodeCost actualCost = lookup.getCost(memo.getNode(memo.getRootGroup()), queryRunner.getDefaultSession(), ImmutableMap.of());
        PlanNodeCost expectedCost = PlanNodeCost.builder().setOutputRowCount(new Estimate(3)).build();
        assertEquals(actualCost, expectedCost);
    }

    private GenericNode node(PlanNodeId id, PlanNode... children)
    {
        return new GenericNode(id, ImmutableList.copyOf(children));
    }

    private GenericNode node(PlanNode... children)
    {
        return node(idAllocator.getNextId(), children);
    }

    private static class GenericNode
            extends PlanNode
    {
        private final List<PlanNode> sources;

        public GenericNode(PlanNodeId id, List<PlanNode> sources)
        {
            super(id);
            this.sources = ImmutableList.copyOf(sources);
        }

        @Override
        public List<PlanNode> getSources()
        {
            return sources;
        }

        @Override
        public List<Symbol> getOutputSymbols()
        {
            return ImmutableList.of();
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            return new GenericNode(getId(), newChildren);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            GenericNode other = (GenericNode) obj;
            return Objects.equals(this.getId(), other.getId())
                    && Objects.equals(this.getSources(), other.getSources());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getId(), getSources());
        }
    }

    private static class NodeCountingCostCalculator
            implements CostCalculator
    {
        @Override
        public PlanNodeCost calculateCost(PlanNode planNode, List<PlanNodeCost> sourceCosts, Session session, Map<Symbol, Type> types)
        {
            double outputRows = 1;
            for (PlanNodeCost sourceCost : sourceCosts) {
                if (!sourceCost.getOutputRowCount().isValueUnknown()) {
                    outputRows += sourceCost.getOutputRowCount().getValue();
                }
            }
            return PlanNodeCost.builder()
                    .setOutputRowCount(new Estimate(outputRows))
                    .build();
        }
    }
}
