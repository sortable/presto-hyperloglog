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
package com.sortable.presto.hyperloglog;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class HyperLogLogStateFactory
        implements AccumulatorStateFactory<HyperLogLogState>
{
    @Override
    public HyperLogLogState createSingleState()
    {
        return new SingleHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getSingleStateClass()
    {
        return SingleHyperLogLogState.class;
    }

    @Override
    public HyperLogLogState createGroupedState()
    {
        return new GroupedHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getGroupedStateClass()
    {
        return GroupedHyperLogLogState.class;
    }

    public static class GroupedHyperLogLogState
            implements GroupedAccumulatorState, HyperLogLogState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedHyperLogLogState.class).instanceSize();
        private final ObjectBigArray<byte[]> hlls = new ObjectBigArray<>();
        private long size;
        private long groupId;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            hlls.ensureCapacity(size);
        }

        @Override
        public byte[] getBytes()
        {
            return hlls.get(groupId);
        }

        @Override
        public void setBytes(byte[] value)
        {
            requireNonNull(value, "value is null");
            hlls.set(groupId, value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + hlls.sizeOf();
        }
    }

    public static class SingleHyperLogLogState
            implements HyperLogLogState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleHyperLogLogState.class).instanceSize();
        private byte[] bytes;

        @Override
        public byte[] getBytes()
        {
            return bytes;
        }

        @Override
        public void setBytes(byte[] value)
        {
            bytes = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long size = INSTANCE_SIZE;
            if (bytes != null) {
                size += SizeOf.sizeOfByteArray(bytes.length);
            }
            return size;
        }
    }
}
