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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.BasicSliceInput;
import java.util.ArrayList;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

@AggregationFunction("merge_hll")
public final class HyperLogLogAggregation
{
    private HyperLogLogAggregation() {}

    private static final int VALUE_BITS = 6;
    private static final int VALUE_MASK = (1 << VALUE_BITS) - 1;
    private static final int EXTENDED_PREFIX_BITS = Integer.SIZE - VALUE_BITS;

    private static final int BITS_PER_BUCKET = 4;
    private static final int MAX_DELTA = (1 << BITS_PER_BUCKET) - 1;

    private static int decodeBucketIndex(int indexBitLength, int entry)
    {
        return entry >>> (Integer.SIZE - indexBitLength);
    }

    private static int decodeBucketValue(int entry)
    {
        return entry & VALUE_MASK;
    }

    private static void insert(byte[] bytes, int bucket, int value)
    {
        if (bytes[bucket] < value) {
            bytes[bucket] = (byte) value;
        }
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.HYPER_LOG_LOG) Slice value)
    {
        byte[] previous = state.getBytes();

        BasicSliceInput input = value.getInput();
        byte format = input.readByte();
        byte p = input.readByte();
        int bucketSize = 1 << p;

        if (previous == null) {
            previous = new byte[bucketSize];
            state.setBytes(previous);
        }
        else if (previous.length != bucketSize) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "conflicted HyperLogLog precision");
        }

        // sparse v2
        if (format == 2) {
            short numberOfEntries = input.readShort();
            for (int i = 0; i < numberOfEntries; i++) {
                int entry = input.readInt();
                int bucket = decodeBucketIndex(p, entry);
                int zeros = Integer.numberOfLeadingZeros(entry << p);
                int bits = EXTENDED_PREFIX_BITS - p;
                if (zeros > bits) {
                    zeros = bits + decodeBucketValue(entry);
                }
                insert(previous, bucket, zeros + 1);
            }
            return;
        }

        // dense v2
        if (format == 3) {
            int baseline = input.readByte();
            for (int i = 0; i < (bucketSize / 2); i++) {
                byte twoDeltas = input.readByte();
                int firstValue = (twoDeltas >> 4) + baseline;
                int secondValue = (twoDeltas & 15) + baseline;
                insert(previous, 2 * i, firstValue);
                insert(previous, 2 * i + 1, secondValue);
            }

            int overflows = input.readUnsignedShort();
            int[] overflowBuckets = new int[overflows];
            for (int i = 0; i < overflows; i++) {
                overflowBuckets[i] = input.readUnsignedShort();
            }
            for (int i = 0; i < overflows; i++) {
                insert(previous, overflowBuckets[i], baseline + MAX_DELTA + input.readByte());
            }

            return;
        }

        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "invalid HyperLogLog format");
    }

    @CombineFunction
    public static void combine(HyperLogLogState state, HyperLogLogState otherState)
    {
        byte[] first = state.getBytes();
        byte[] second = otherState.getBytes();

        if (first == null) {
            state.setBytes(second);
            return;
        }

        if (second == null) {
            return;
        }

        int firstLength = first.length;

        if (firstLength != second.length) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "conflicted HyperLogLog precision");
        }

        for (int i = 0; i < firstLength; i++) {
            insert(first, i, second[i]);
        }
    }

    @OutputFunction(StandardTypes.P4_HYPER_LOG_LOG)
    public static void output(HyperLogLogState state, BlockBuilder out)
    {
        byte[] bytes = state.getBytes();
        int p = (int) (Math.log(bytes.length) / Math.log(2));
        int baseline = 0;

        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] > baseline) {
                baseline = bytes[i];
            }
        }

        out.writeByte(3);
        out.writeByte(p);
        out.writeByte(baseline);

        int firstDelta = 0;

        ArrayList<Integer> overflowsBuckets = new ArrayList<>();
        ArrayList<Integer> overflowsValues = new ArrayList<>();

        // todo: sort overflows
        // todo: little endian ?
        // todo: use DynamicSliceOutput ?

        for (int i = 0; i < bytes.length; i++) {
            int delta = bytes[i] - baseline;
            if (delta > MAX_DELTA) {
                overflowsBuckets.add(i);
                overflowsValues.add(delta - MAX_DELTA);
                delta = MAX_DELTA;
            }
            if (i % 2 == 0) {
                firstDelta = delta;
            }
            else {
                out.writeByte((((byte) firstDelta) << 4) | ((byte) delta));
            }
        }

        int overflows = overflowsBuckets.size();
        out.writeShort(overflows);

        for (int i = 0; i < overflows; i++) {
            out.writeShort(overflowsBuckets.get(i));
        }

        for (int i = 0; i < overflows; i++) {
            out.writeByte(overflowsValues.get(i));
        }

        out.closeEntry();
    }
}
