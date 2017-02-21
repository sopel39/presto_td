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
package com.facebook.presto.tests;

import com.facebook.presto.tests.statistics.Metric;
import com.facebook.presto.tests.statistics.MetricComparison;
import com.facebook.presto.tests.statistics.StatisticsAssertion;
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.DIFFER;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithoutCatalogs;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.IntStream.rangeClosed;

public class TestDisplayTpchDistributedStats
{
    private static final int NUMBER_OF_TPCH_QUERIES = 22;

    private final StatisticsAssertion statisticsAssertion;

    public TestDisplayTpchDistributedStats()
            throws Exception
    {
        DistributedQueryRunner runner = createQueryRunnerWithoutCatalogs(emptyMap(), emptyMap());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of(
                "tpch.column-naming", ColumnNaming.STANDARD.name()
        ));
        statisticsAssertion = new StatisticsAssertion(runner);
    }

    /**
     * This is a development tool for manual inspection of differences between
     * cost estimates and actual execution costs. Its outputs need to be inspected
     * manually because at this point no sensible assertions can be formulated
     * for the entirety of TPCH queries.
     */
    @Test
    void testCostEstimatesVsRealityDifferences()
    {
        rangeClosed(1, NUMBER_OF_TPCH_QUERIES)
                .filter(i -> i != 15) //query 15 creates a view, which TPCH connector does not support.
                .forEach(i -> summarizeQuery(i, getTpchQuery(i)));
    }

    private String getTpchQuery(int i)
    {
        try {
            String queryClassPath = "/io/airlift/tpch/queries/q" + i + ".sql";
            return Resources.toString(getClass().getResource(queryClassPath), UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void summarizeQuery(int queryNumber, String query)
    {
        System.out.println(format("Query TPCH [%s].\n", queryNumber));

        List<MetricComparison> comparisons = statisticsAssertion.metricComparisons(query);

        Map<Metric, Map<MetricComparison.Result, List<MetricComparison>>> metricSummaries =
                comparisons.stream()
                        .collect(groupingBy(MetricComparison::getMetric, groupingBy((metricComparison) -> metricComparison.result(defaultTolerance()))));

        metricSummaries.forEach((metricName, resultSummaries) -> {
            int resultsCount = resultSummaries.values()
                    .stream()
                    .mapToInt(List::size)
                    .sum();
            System.out.println(format("Summary for metric [%s] contains [%s] results", metricName, resultsCount));
            outputSummary(resultSummaries, NO_ESTIMATE);
            outputSummary(resultSummaries, NO_BASELINE);
            outputSummary(resultSummaries, DIFFER);
            outputSummary(resultSummaries, MATCH);
            System.out.println();
        });

        System.out.println("Detailed results:\n");

        comparisons.forEach(System.out::println);
    }

    private void outputSummary(Map<MetricComparison.Result, List<MetricComparison>> resultSummaries, MetricComparison.Result result)
    {
        System.out.println(format("[%s]\t-\t[%s]", result, resultSummaries.getOrDefault(result, emptyList()).size()));
    }
}