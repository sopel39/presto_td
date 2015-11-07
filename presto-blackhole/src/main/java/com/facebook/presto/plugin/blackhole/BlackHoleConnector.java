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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

public class BlackHoleConnector
        implements Connector
{
    public static final String SPLIT_COUNT_PROPERTY = "split_count";
    public static final String PAGES_PER_SPLIT_PROPERTY = "pages_per_split";
    public static final String ROWS_PER_PAGE_PROPERTY = "rows_per_page";
    public static final String FIELD_LENGTH_PROPERTY = "field_length";
    public static final String DISTRIBUTED_ON = "distributed_on";
    public static final String PAGE_PROCESSING_DELAY = "page_processing_delay";

    private final BlackHoleMetadata metadata;
    private final BlackHoleSplitManager splitManager;
    private final BlackHolePageSourceProvider pageSourceProvider;
    private final BlackHolePageSinkProvider pageSinkProvider;
    private final BlackHoleNodePartitioningProvider partitioningProvider;
    private final TypeManager typeManager;
    private final ExecutorService executorService;

    public BlackHoleConnector(
            BlackHoleMetadata metadata,
            BlackHoleSplitManager splitManager,
            BlackHolePageSourceProvider pageSourceProvider,
            BlackHolePageSinkProvider pageSinkProvider,
            BlackHoleNodePartitioningProvider partitioningProvider,
            TypeManager typeManager,
            ExecutorService executorService)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
        this.partitioningProvider = partitioningProvider;
        this.typeManager = typeManager;
        this.executorService = executorService;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return BlackHoleTransactionHandle.INSTANCE;
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        // TODO: support transactional metadata
        return true;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                integerSessionProperty(
                        SPLIT_COUNT_PROPERTY,
                        "Number of splits generated by this table",
                        0,
                        false),
                integerSessionProperty(
                        PAGES_PER_SPLIT_PROPERTY,
                        "Number of pages per each split generated by this table",
                        0,
                        false),
                integerSessionProperty(
                        ROWS_PER_PAGE_PROPERTY,
                        "Number of rows per each page generated by this table",
                        0,
                        false),
                integerSessionProperty(
                        FIELD_LENGTH_PROPERTY,
                        "Overwrite default length (16) of variable length columns, such as VARCHAR or VARBINARY",
                        16,
                        false),
                new PropertyMetadata<>(
                        DISTRIBUTED_ON,
                        "Distribution columns",
                        typeManager.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()))),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((List<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toList())),
                        List.class::cast),
                new PropertyMetadata<>(
                        PAGE_PROCESSING_DELAY,
                        "Sleep duration before processing each page",
                        VARCHAR,
                        Duration.class,
                        Duration.valueOf("0s"),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString));
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return partitioningProvider;
    }

    @Override
    public void shutdown()
    {
        executorService.shutdownNow();
    }
}
