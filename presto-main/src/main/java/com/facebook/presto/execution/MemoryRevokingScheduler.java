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
package com.facebook.presto.execution;

import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(task -> task.getTaskInfo().getStats().getCreateTime());
    private final List<MemoryPool> memoryPools;

    @Inject
    public MemoryRevokingScheduler(LocalMemoryManager localMemoryManager)
    {
        requireNonNull(localMemoryManager, "localMemoryManager can not be null");
        memoryPools = ImmutableList.of(localMemoryManager.getPool(LocalMemoryManager.GENERAL_POOL), localMemoryManager.getPool(LocalMemoryManager.RESERVED_POOL));
    }

    @VisibleForTesting
    MemoryRevokingScheduler(List<MemoryPool> memoryPools)
    {
        this.memoryPools = ImmutableList.copyOf(memoryPools);
    }

    public void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks)
    {
        memoryPools.forEach(memoryPool -> requestMemoryRevokingIfNeeded(sqlTasks, memoryPool));
    }

    private void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        long freeBytes = memoryPool.getFreeBytes();
        if (freeBytes > 0) {
            return;
        }

        long remainingBytesToRevoke = -freeBytes;
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(sqlTasks, memoryPool);
        requestRevoking(remainingBytesToRevoke, sqlTasks, memoryPool);
    }

    private long getMemoryAlreadyBeingRevoked(Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        AtomicLong memoryAlreadyBeingRevoked = new AtomicLong();
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
                .forEach(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<Void, Void>()
                {
                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, Void context)
                    {
                        if (operatorContext.isMemoryRevokingRequested()) {
                            memoryAlreadyBeingRevoked.addAndGet(operatorContext.getReservedRevocableBytes());
                        }
                        return null;
                    }
                }, null));
        return memoryAlreadyBeingRevoked.get();
    }

    private void requestRevoking(long remainingBytesToRevoke, Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
                .sorted(ORDER_BY_CREATE_TIME)
                .forEach(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<AtomicLong, Void>()
                {
                    @Override
                    public Void visitQueryContext(QueryContext queryContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() < 0) {
                            // exit immediately if no work needs to be done
                            return null;
                        }
                        return super.visitQueryContext(queryContext, remainingBytesToRevoke);
                    }

                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() > 0) {
                            long operatorRevocableBytes = operatorContext.getReservedRevocableBytes();
                            if (operatorRevocableBytes > 0 && !operatorContext.isMemoryRevokingRequested()) {
                                operatorContext.requestMemoryRevoking();
                                remainingBytesToRevoke.addAndGet(-operatorRevocableBytes);
                                log.info("(%s)requested revoking %s; remaining %s", memoryPool.getId(), operatorRevocableBytes, remainingBytesToRevoke.get());
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevokeAtomic));
    }
}
