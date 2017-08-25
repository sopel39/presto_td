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
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.SymbolAliasUtil.aliasAliasMapMatches;
import static com.facebook.presto.sql.planner.assertions.SymbolAliasUtil.aliasSetListMatches;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class GroupIdMatcher
        implements Matcher
{
    private final List<Set<SymbolAlias>> groups;
    private final Map<SymbolAlias, SymbolAlias> identityMappings;
    private final SymbolAlias groupIdAlias;

    public GroupIdMatcher(List<Set<SymbolAlias>> groups, Map<SymbolAlias, SymbolAlias> identityMappings, SymbolAlias groupIdAlias)
    {
        this.groups = groups;
        this.identityMappings = identityMappings;
        this.groupIdAlias = groupIdAlias;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof GroupIdNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeCost cost, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        GroupIdNode groudIdNode = (GroupIdNode) node;
        List<List<Symbol>> actualGroups = groudIdNode.getGroupingSets();
        Map<Symbol, Symbol> actualArgumentMappings = groudIdNode.getArgumentMappings();

        if (!aliasSetListMatches(
                groups,
                actualGroups.stream()
                        .map(ImmutableSet::copyOf)
                        .collect(toImmutableList()),
                symbolAliases)) {
            return NO_MATCH;
        }

        if (!aliasAliasMapMatches(identityMappings, actualArgumentMappings, symbolAliases)) {
            return NO_MATCH;
        }

        return match(groupIdAlias.toString(), groudIdNode.getGroupIdSymbol().toSymbolReference());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groups", groups)
                .toString();
    }
}
