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
package com.facebook.presto.operator;

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.Menago.SpillingStateSnapshot;
import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MappedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator, Closeable
{
    private final LookupJoiner lookupJoiner;
    private final OperatorContext operatorContext;

    private final List<Type> allTypes;
    private final List<Type> probeTypes;

    private Menago menago;
    private final LookupSourceFactory lookupSourceFactory;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final boolean probeOnOuterSide;

    private Optional<SpilledLookupJoiner> spilledLookupJoiner = Optional.empty();

    private boolean finishing;
    private boolean closed;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<Iterator<Partition<LookupSource>>> lookupPartitions = Optional.empty();

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> allTypes,
            List<Type> probeTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.allTypes = ImmutableList.copyOf(requireNonNull(allTypes, "allTypes is null"));
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");

        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");

        ListenableFuture<LookupSource> lookupSourceFuture = lookupSourceFactory.createLookupSource();
        this.probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
        this.lookupJoiner = new LookupJoiner(allTypes, lookupSourceFuture, joinProbeFactory, probeOnOuterSide);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return allTypes;
    }

    @Override
    public void finish()
    {
        lookupJoiner.finish();

        //The `hasSpilled()` value below is determined before the operator starts its work.
        //Hence, there's no race between an operator finishing early and a spill occurring
        //during other LookupJoinOperator operator's work. At least, at the moment of writing of this comment...
        if (!finishing && lookupSourceFactory.hasSpilled()) {
            int consumersCount = lookupJoinsCount.orElseThrow(() -> new IllegalStateException("Indeterminate number of LookupJoinOperator-s when using spill to disk. This is a bug."));
            lookupPartitions = Optional.of(lookupSourceFactory.beginLookupSourceUnspilling(consumersCount));
            ensureSpillerLoaded();
        }
        finishing = true;
    }

    private void unspillNextLookupSource()
    {
        checkState(spiller.isPresent());
        checkState(lookupPartitions.isPresent());

        spilledLookupJoiner.ifPresent(joiner -> {
            joiner.finish();
        });

        if (lookupPartitions.get().hasNext()) {
            Partition<LookupSource> currentSpilledPartition = lookupPartitions.get().next();

            LookupJoiner nextLookupJoiner = new LookupJoiner(allTypes, toListenableFuture(currentSpilledPartition.load()), joinProbeFactory, probeOnOuterSide);
            spilledLookupJoiner = Optional.of(new SpilledLookupJoiner(
                    currentSpilledPartition,
                    nextLookupJoiner,
                    spiller.get().getSpilledPages(currentSpilledPartition.number())
            ));
        }
        else {
            spiller.get().close();
            spiller = Optional.empty();
        }
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = lookupJoiner.isFinished() && !spiller.isPresent();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!spillInProgress.isDone()) {
            return spillInProgress;
        }
        if (spilledLookupJoiner.isPresent()) {
            return spilledLookupJoiner.get().isBlocked();
        }
        return lookupJoiner.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return lookupJoiner.needsInput() && !lookupJoiner.hasRejectedInputPage();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        SpillingStateSnapshot spillingState = menago.getSpillingState();
        if (spillingState.hasSpilled()) {
            page = spillAndMaskSpilledPositions(page, spillingState);
            if (page.getPositionCount() == 0) {
                return;
            }
        }

        lookupJoiner.addInput(page, spillingState);
    }

    private Page spillAndMaskSpilledPositions(Page page, SpillingStateSnapshot spillingState)
    {
        ensureSpillerLoaded();
        checkState(spillInProgress.isDone(), "previous spill still in progress");

        PartitioningSpiller.PartitioningSpillResult spillResult = spiller.get().partitionAndSpill(page, spillingState);

        if (!spillResult.isBlocked().isDone()) {
            this.spillInProgress = spillResult.isBlocked();
        }
        IntArrayList unspilledPositions = spillResult.getUnspilledPositions();

        return mapPage(unspilledPositions, page);
    }

    private void ensureSpillerLoaded()
    {
        checkState(lookupSourceFactory.hasSpilled());
        if (!spiller.isPresent()) {
            spiller = Optional.of(lookupSourceFactory.createProbeSpiller(operatorContext, probeTypes, hashGenerator));
        }
    }

    private Page mapPage(IntArrayList unspilledPositions, Page page)
    {
        Block[] mappedBlocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            mappedBlocks[channel] = new MappedBlock(page.getBlock(channel), unspilledPositions.elements());
        }
        return new Page(mappedBlocks);
    }

    @Override
    public Page getOutput()
    {
        if (!finishing || !lookupJoiner.isFinished()) {
            if (lookupJoiner.needsInput()) {
                lookupJoiner.takeRejectedInputPage().ifPresent(this::addInput);
            }

            return lookupJoiner.getOutput();
        }

        if (spiller.isPresent()) {

            if (!spilledLookupJoiner.isPresent() || spilledLookupJoiner.get().isFinished()) {
                unspillNextLookupSource();
                return null;
            }
            SpilledLookupJoiner joiner = spilledLookupJoiner.get();
            return joiner.getOutput();
        }

        return null;
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        lookupJoiner.close();
        onClose.run();
    }
}
