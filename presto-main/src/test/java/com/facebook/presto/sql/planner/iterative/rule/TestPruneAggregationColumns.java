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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class TestPruneAggregationColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
            throws Exception
    {
        tester().assertThat(new PruneAggregationColumns())
                .on(p -> buildProjectedAggregation(p, symbol -> symbol.getName().equals("b")))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("b")),
                                aggregation(
                                        ImmutableList.of(ImmutableSet.of("key")),
                                        ImmutableMap.of(
                                                Optional.of("b"),
                                                functionCall("count", false, ImmutableList.of())),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("key"))));
    }

    @Test
    public void testAllOutputsReferenced()
            throws Exception
    {
        tester().assertThat(new PruneAggregationColumns())
                .on(p -> buildProjectedAggregation(p, alwaysTrue()))
                .doesNotFire();
    }

    private ProjectNode buildProjectedAggregation(PlanBuilder planBuilder, Predicate<Symbol> projectionFilter)
    {
        Symbol a = planBuilder.symbol("a");
        Symbol b = planBuilder.symbol("b");
        Symbol key = planBuilder.symbol("key");
        return planBuilder.project(
                Assignments.identity(ImmutableList.of(a, b).stream().filter(projectionFilter).collect(toImmutableSet())),
                planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                        .source(planBuilder.values(key))
                        .addGroupingSet(key)
                        .addAggregation(a, planBuilder.expression("count()"), ImmutableList.of())
                        .addAggregation(b, planBuilder.expression("count()"), ImmutableList.of())));
    }
}
