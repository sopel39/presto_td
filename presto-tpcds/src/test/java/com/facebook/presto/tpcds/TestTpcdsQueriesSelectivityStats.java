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
package com.facebook.presto.tpcds;

import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.statistics.StatisticsAssertion;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.Metrics.OUTPUT_ROW_COUNT;

/**
 * The tests in this class have been written to ensure that the statistics code
 * does not introduce regressions in selectivity estimates of table rows in
 * TPC-DS queries in future. The disabled tests fail in 0.179-t. They
 * should be enabled later as the selectivity estimates improve over time.
 */
public class TestTpcdsQueriesSelectivityStats
{
    private final StatisticsAssertion statisticsAssertion;

    public TestTpcdsQueriesSelectivityStats()
            throws Exception
    {
        DistributedQueryRunner distributedQueryRunner = TpcdsQueryRunner.createQueryRunner();

        statisticsAssertion = new StatisticsAssertion(distributedQueryRunner);
    }

    @Test
    void testVarcharEqualityq77()
    {
        statisticsAssertion.check(
                "select * from customer_demographics where cd_marital_status = 'D'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testDecimalEqualityq60()
    {
        statisticsAssertion.check(
                " select * from customer_address where (\"ca_gmt_offset\" = -5)",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testVarcharInq60()
    {
        statisticsAssertion.check(
                "select * from item where i_category IN ('MUSIC')",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }
}
