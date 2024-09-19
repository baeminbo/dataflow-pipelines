package baeminbo;

import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An unbounded source generating the long value from `start` (inclusive) to `end` (exclusive). An element is emitted
 * for every 1 second. The timestamp and watermark are set to the current time (`Instant.now()`), but from the
 * `lastElementStart` (inclusive), the timestamp doesn't advance so that they become "late elements".
 */
public class UnboundedLateElementSource extends UnboundedSource<Long, UnboundedLateElementSource.Checkpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(UnboundedLateElementSource.class);
    private static final Duration ELEMENT_GENERATION_INTERVAL = Duration.standardSeconds(1);

    private final long start;
    private final long end;
    private final long lateElementStart;

    public UnboundedLateElementSource(long start, long end, long lastElementStart) {
        Preconditions.checkArgument(start <= end);
        this.start = start;
        this.end = end;
        this.lateElementStart = lastElementStart;
    }

    @Override
    public List<UnboundedLateElementSource> split(int desiredNumSplits, PipelineOptions options) {
        return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Long> createReader(PipelineOptions options, @Nullable Checkpoint checkpoint) {
        if (checkpoint == null) {
            return new Reader(start);
        } else {
            return new Reader(checkpoint.getNext(), checkpoint.getLastTimestamp(), checkpoint.getLastWatermark());
        }
    }

    @Override
    public Coder<Checkpoint> getCheckpointMarkCoder() {
        return SerializableCoder.of(Checkpoint.class);
    }

    @Override
    public Coder<Long> getOutputCoder() {
        return VarLongCoder.of();
    }

    public class Reader extends UnboundedReader<Long> {
        private long current;
        private Instant timestamp;
        private Instant watermark;
        private Instant lastAdvancedTime;

        public Reader(long current) {
            this(current, null, null);
        }

        public Reader(long current, @Nullable Instant lastTimeStamp, @Nullable Instant lastWatermark) {
            Preconditions.checkArgument(current > Long.MIN_VALUE); //  due to --current in start();
            this.current = current;
            this.timestamp = lastTimeStamp == null ? Instant.EPOCH : lastTimeStamp;
            this.watermark = lastWatermark == null ? Instant.EPOCH : lastWatermark;
            this.lastAdvancedTime = Instant.EPOCH;
        }

        @Override
        public boolean start() {
            Preconditions.checkArgument(current > Long.MIN_VALUE);
            --current;
            return advance();
        }

        @Override
        public boolean advance() {
            if (current < end - 1) {
                Instant now = Instant.now();
                if (now.isBefore(lastAdvancedTime.plus(ELEMENT_GENERATION_INTERVAL))) {
                    return false;
                }

                ++current;
                watermark = now;
                if (current < lateElementStart) {
                    timestamp = now;
                } else {
                    LOG.info("Set late timestamp:{} for element:{} at watermark:{}", timestamp, current, watermark);
                }

                lastAdvancedTime = now;
                return true;
            } else {
                watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
                return false;
            }
        }

        @Override
        public Long getCurrent() {
            return current;
        }

        @Override
        public Instant getCurrentTimestamp() {
            return timestamp;
        }

        @Override
        public void close() {

        }

        @Override
        public Instant getWatermark() {
            return watermark;
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return new Checkpoint(current + 1, timestamp, watermark);
        }

        @Override
        public UnboundedLateElementSource getCurrentSource() {
            return UnboundedLateElementSource.this;
        }
    }

    public static class Checkpoint implements CheckpointMark, Serializable {
        private final long next;
        private final Instant lastTimestamp;
        private final Instant lastWatermark;

        public Checkpoint(long next, Instant lastTimestamp, Instant lastWatermark) {
            this.next = next;
            this.lastTimestamp = lastTimestamp;
            this.lastWatermark = lastWatermark;
        }

        public long getNext() {
            return next;
        }

        public Instant getLastTimestamp() {
            return lastTimestamp;
        }

        public Instant getLastWatermark() {
            return lastWatermark;
        }

        @Override
        public void finalizeCheckpoint() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Checkpoint that = (Checkpoint) o;
            return next == that.next && Objects.equals(lastTimestamp, that.lastTimestamp) && Objects.equals(lastWatermark, that.lastWatermark);
        }

        @Override
        public int hashCode() {
            return Objects.hash(next, lastTimestamp, lastWatermark);
        }

        @Override
        public String toString() {
            return "Checkpoint{" +
                    "next=" + next +
                    ", lastTimestamp=" + lastTimestamp +
                    ", lastWatermark=" + lastWatermark +
                    '}';
        }
    }
}
