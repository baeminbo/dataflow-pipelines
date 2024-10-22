package baeminbo;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MapStatePipeline {
    private static final Logger LOG = LoggerFactory.getLogger(MapStatePipeline.class);

    private static final long SOURCE_START = 0;
    private static final long SOURCE_END = 40; // exclusive
    private static final Duration SOURCE_FREQUENCY = Duration.standardSeconds(5);
    private static final String STATE_ID = "myState";


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(Read.from(new MySource(SOURCE_START, SOURCE_END)))
                .apply(WithKeys.of(""))
                .apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
                    @StateId(STATE_ID)
                    private final StateSpec<MapState<String, Long>> myStateSpec = StateSpecs.map();

                    @ProcessElement
                    public void processElement(ProcessContext c,
                                               BoundedWindow window,
                                               @StateId(STATE_ID) MapState<String, Long> myState) {
                        ArrayList<Map.Entry<String, Long>> stateValues = Lists.newArrayList(myState.entries().read());
                        long value = c.element().getValue();
                        LOG.info("Process element:{}, timestamp:{}, window:{}, currentStateValues:{}",
                                value, c.timestamp(), window, stateValues);

                        if (value == 0) {
                            LOG.info("clear state");
                            myState.clear();
                        } else if (value < 3) {
                            String key = "state-" + value;
                            LOG.info("put key:{}, value:{}", key, value);
                            myState.put(key, value);
                        } else {
                            String key = "state-1";
                            LOG.info("remove key:{}", key);
                            myState.remove(key);
                        }
                    }
                }));

        pipeline.run();
    }


    public static class MySource extends UnboundedSource<Long, MySource.MyCheckpoint> {
        private final long start;
        private final long end;

        public MySource(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public List<MySource> split(int desiredNumSplits, PipelineOptions options) {
            return Collections.singletonList(this);
        }

        @Override
        public UnboundedReader<Long> createReader(PipelineOptions options, @Nullable MyCheckpoint checkpoint) {
            return new MyReader(checkpoint == null ? start : checkpoint.getNext());
        }

        @Override
        public Coder<MyCheckpoint> getCheckpointMarkCoder() {
            return SerializableCoder.of(MyCheckpoint.class);
        }

        @Override
        public Coder<Long> getOutputCoder() {
            return VarLongCoder.of();
        }

        public static class MyCheckpoint implements CheckpointMark, Serializable {
            private final long next;

            public MyCheckpoint(long next) {
                this.next = next;
            }

            @Override
            public void finalizeCheckpoint() {
            }

            public long getNext() {
                return next;
            }
        }

        public class MyReader extends UnboundedReader<Long> {
            private long current;
            private Instant timestamp;
            private Instant watermark;
            private Instant lastAdvancedTime;

            public MyReader(long current) {
                this.current = current;
            }

            @Override
            public boolean start() {
                Instant now = Instant.now();
                if (current < end) {
                    timestamp = now;
                    watermark = now;
                    lastAdvancedTime = now;
                    return true;
                } else {
                    watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
                    return false;
                }
            }

            @Override
            public boolean advance() {
                Instant now = Instant.now();
                if (current + 1 >= end) {
                    // Reach the end. Set watermark to max.
                    watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
                    return false;
                } else if (now.isBefore(lastAdvancedTime.plus(SOURCE_FREQUENCY))) {
                    // Not a time for emitting. Update watermark only
                    watermark = now;
                    return false;
                } else {
                    ++current;
                    timestamp = now;
                    watermark = now;
                    lastAdvancedTime = now;
                    return true;
                }
            }

            @Override
            public Long getCurrent() {
                return current;
            }

            @Override
            public Instant getCurrentTimestamp() {
                Preconditions.checkNotNull(timestamp,
                        String.format("timestamp is null for current:%d in [%d, %d)", current, start, end));
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
                return new MyCheckpoint(current + 1);
            }

            @Override
            public UnboundedSource<Long, MyCheckpoint> getCurrentSource() {
                return MySource.this;
            }
        }
    }
}
