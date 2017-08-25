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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.SymbolAliasUtil.aliasSetListMatches;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AggregationMatcher
        implements Matcher
{
    private final Map<Symbol, Symbol> masks;
    private final List<Set<SymbolAlias>> groupingSets;
    private final Optional<Symbol> groupId;
    private final Step step;

    public AggregationMatcher(List<Set<SymbolAlias>> groupingSets, Map<Symbol, Symbol> masks, Optional<Symbol> groupId, Step step)
    {
        this.masks = masks;
        this.groupingSets = groupingSets;
        this.groupId = groupId;
        this.step = step;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof AggregationNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeCost cost, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        AggregationNode aggregationNode = (AggregationNode) node;

        if (groupId.isPresent() != aggregationNode.getGroupIdSymbol().isPresent()) {
            return NO_MATCH;
        }

        if (!aliasSetListMatches(
                groupingSets,
                aggregationNode.getGroupingSets().stream()
                        .map(ImmutableSet::copyOf)
                        .collect(toImmutableList()),
                symbolAliases)) {
            return NO_MATCH;
        }

        List<Symbol> aggregationsWithMask = aggregationNode.getAggregations()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getCall().isDistinct())
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());

        if (aggregationsWithMask.size() != masks.keySet().size()) {
            return NO_MATCH;
        }

        for (Symbol symbol : aggregationsWithMask) {
            if (!masks.keySet().contains(symbol)) {
                return NO_MATCH;
            }
        }

        if (step != aggregationNode.getStep()) {
            return NO_MATCH;
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupingSets", groupingSets)
                .add("masks", masks)
                .add("groudId", groupId)
                .add("step", step)
                .toString();
    }
}
