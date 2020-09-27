package baeminbo;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleGbkPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleGbkPipeline.class);

  private static final long START_ELEMENT = 0; // inclusive
  private static final long END_ELEMENT = 1000; // exclusive
  private static final long SHARD_COUNT = 10;
  private static final int READ_SLEEP_MILLIS = 100; // sleep 100 milliseconds between elements in READ
  private static final String PRINT_STEP_MODE = "GBK"; // See enum PrintStepMode
  private static final String SOURCE_MODE = "BOUNDED";  // See num SourceMode
  private static final long WINDOW_SECONDS = 5; // Window duration in FixedWindows

  public enum SourceMode {
    BOUNDED,
    UNBOUNDED,
  }

  public enum PrintStepMode {
    GBK,
    STATEFUL,
  }

  @SuppressWarnings("unused")
  public interface PrintPipelineOptions extends PipelineOptions {

    @Description("Start element (inclusive)")
    @Default.Long(START_ELEMENT)
    ValueProvider<Long> getStartElement();

    void setStartElement(ValueProvider<Long> startElement);

    @Description("End element (exclusive)")
    @Default.Long(END_ELEMENT)
    ValueProvider<Long> getEndElement();

    void setEndElement(ValueProvider<Long> endElement);

    @Description("Number of keys in 'ConvertToKV' step")
    @Default.Long(SHARD_COUNT)
    ValueProvider<Long> getShardCount();

    void setShardCount(ValueProvider<Long> shardCount);

    @Description("Sleep time in milliseconds between elements in 'ToKV' step")
    @Default.Long(READ_SLEEP_MILLIS)
    ValueProvider<Long> getReadSleepMillis();

    void setReadSleepMillis(ValueProvider<Long> readSleepMillis);

    @Description("Print mode: 'GBK' or 'STATEFUL'")
    @Default.Enum(PRINT_STEP_MODE)
    PrintStepMode getPrintStepMode();

    void setPrintStepMode(PrintStepMode printStepMode);

    @Description("Source mode: 'BOUNDED' or 'UNBOUNDED")
    @Default.Enum(SOURCE_MODE)
    SourceMode getSourceMode();

    void setSourceMode(SourceMode sourceMode);

    // We cannot have the window size as ValueProvider since FixedWindows doesn't receive
    // a ValueProvider.
    @Description("Window size in seconds for 'GBK' mode or timer offset for 'STATEFUL' mode")
    @Default.Long(WINDOW_SECONDS)
    long getWindowSeconds();

    void setWindowSeconds(long windowSizeSeconds);
  }

  public static class LongBoundedSource extends BoundedSource<Long> {

    private static final int BYTE_COUNT_PER_ELEMENT = 8; // assuming a long element is encoded into 8 bytes.

    private final ValueProvider<Long> start;
    private final ValueProvider<Long> end;

    public LongBoundedSource(ValueProvider<Long> start, ValueProvider<Long> end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public List<? extends BoundedSource<Long>> split(long desiredBundleSizeBytes,
        PipelineOptions options) {
      // Do not split
      LOG.info("Source. Split recommended but do not split");
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      if (start.isAccessible() && end.isAccessible()) {
        return start.get() <= end.get() ? (end.get() - start.get()) * BYTE_COUNT_PER_ELEMENT : 0L;
      } else {
        return 0L;
      }
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public LongBoundedReader createReader(PipelineOptions options) {
      LOG.info("Source. Create Reader. start: {}, end: {}", start, end);
      long sleepMillis = options.as(PrintPipelineOptions.class).getReadSleepMillis().get();
      return new LongBoundedReader(this, sleepMillis);
    }

    @Override
    public String toString() {
      return "LongBoundedSource{" +
          "start=" + start +
          ", end=" + end +
          '}';
    }
  }

  public static class LongBoundedReader extends BoundedReader<Long> {

    private final LongBoundedSource source;
    private final long end;
    private final long sleepMillis;
    private long current;

    public LongBoundedReader(LongBoundedSource source, long sleepMillis) {
      this.source = source;
      this.end = source.end.get();
      this.sleepMillis = sleepMillis;
      this.current = source.start.get();
    }

    @Override
    public LongBoundedSource getCurrentSource() {
      // LOG.info("Reader. Get current source: {}", source);
      return source;
    }

    @Override
    public boolean start() {
      LOG.info("Reader. Start. current: {}, end: {}", current, end);
      return current < end;
    }

    @Override
    public boolean advance() {
      if (current < end) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ignored) {
        }

        ++current;
        return current < end;
      } else {
        return false;
      }
    }

    @Override
    public Long getCurrent() {
      return current;
    }

    @Override
    public void close() {
      LOG.info("Reader. Close. current: {}, end: {}", current, end);
    }
  }

  public static class LongUnboundedSource extends UnboundedSource<Long, LongUnboundedCheckpoint> {

    private final ValueProvider<Long> start;

    private final ValueProvider<Long> end;

    public LongUnboundedSource(ValueProvider<Long> start, ValueProvider<Long> end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public List<? extends UnboundedSource<Long, LongUnboundedCheckpoint>> split(
        int desiredNumSplits,
        PipelineOptions options) {
      LOG.info("Source. Split recommended, but do not split");
      return Collections.singletonList(this); // do not split
    }

    @Override
    public LongUnboundedReader createReader(PipelineOptions options,
        @Nullable LongUnboundedCheckpoint checkpoint) {
      LOG.info("Source. Create reader. checkpoint: {}", checkpoint);
      long current = checkpoint != null ? checkpoint.current : start.get();
      long sleepMillis = options.as(PrintPipelineOptions.class).getReadSleepMillis().get();
      return new LongUnboundedReader(this, current, end.get(), sleepMillis);
    }

    @Override
    public Coder<LongUnboundedCheckpoint> getCheckpointMarkCoder() {
      return SerializableCoder.of(LongUnboundedCheckpoint.class);
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public String toString() {
      return "LongUnboundedSource{" +
          "start=" + start +
          ", end=" + end +
          '}';
    }
  }

  public static class LongUnboundedReader extends UnboundedReader<Long> {

    private final LongUnboundedSource source;
    private final long end;
    private final long sleepMillis;

    private long current;

    public LongUnboundedReader(LongUnboundedSource source, long current, long end,
        long sleepMillis) {
      this.source = source;
      this.end = end;
      this.current = current;
      this.sleepMillis = sleepMillis;
    }

    @Override
    public boolean start() {
      LOG.info("Reader. Start. current: {}, end: {}", current, end);
      return current < end;
    }

    @Override
    public boolean advance() {
      if (current < end) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ignored) {
        }
        ++current;
        return current < end;
      } else {
        return false;
      }
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return Instant.now();
    }

    @Override
    public void close() {
      LOG.info("Reader. Close. current: {}, end: {}", current, end);
    }

    @Override
    public Instant getWatermark() {
      if (current < end) {
        return Instant.now();
      } else {
        // may have a similar effect with Job Draining.
        return GlobalWindow.TIMESTAMP_MAX_VALUE;
      }
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      LongUnboundedCheckpoint checkpoint = new LongUnboundedCheckpoint(current);
      LOG.info("Reader. Get checkpoint. checkpoint: {}", checkpoint);
      return checkpoint;
    }

    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
      // LOG.info("Reader. Get current source: {}", source);
      return source;
    }
  }

  public static class LongUnboundedCheckpoint implements CheckpointMark, Serializable {

    private final long current;

    public LongUnboundedCheckpoint(long current) {
      this.current = current;
    }

    @Override
    public void finalizeCheckpoint() {
    }

    @Override
    public String toString() {
      return "LongUnboundedCheckpoint{" +
          "current=" + current +
          '}';
    }
  }

  private static class ConvertToKVFn extends DoFn<Long, KV<String, String>> {
    @SuppressWarnings("unused")
    @StartBundle
    public void startBundle() {
      LOG.info("ConvertToKV. Start Bundle");
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window,
        PipelineOptions options) {

      long inputElement = context.element();
      long shardCount = options.as(PrintPipelineOptions.class).getShardCount().get();

      KV<String, String> outputElement = KV
          .of(Long.toString(Math.floorMod(inputElement, shardCount)),
              Long.toString(inputElement));
      Instant timestamp = Instant.now();

      LOG.info("ConvertToKV. inputElement: {}, outputElement: {}, timestamp: {}, window: {}",
          inputElement, outputElement, timestamp, window);

      context.outputWithTimestamp(outputElement, timestamp);
    }

    @SuppressWarnings("unused")
    @FinishBundle
    public void finishBundle() {
      LOG.info("ConvertToKV. Finish Bundle");
    }
  }

  private static class NonStatefulPrintFn extends DoFn<KV<String, Iterable<String>>, Void> {

    @SuppressWarnings("unused")
    @StartBundle
    public void startBundle() {
      LOG.info("Print. Start bundle");
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      String key = context.element().getKey();
      Iterable<String> values = context.element().getValue();
      Instant timestamp = context.timestamp();
      LOG.info("Print. key: {}, timestamp: {}, window: {}, values: {}",
          key, timestamp, window, values);
    }

    @SuppressWarnings("unused")
    @FinishBundle
    public void finishBundle() {
      LOG.info("Print. End bundle");
    }

  }

  private static class StatefulPrintFn extends DoFn<KV<String, String>, Void> {

    @SuppressWarnings("unused")
    @StateId("key")
    private final StateSpec<ValueState<String>> keyState = StateSpecs.value();
    @SuppressWarnings("unused")
    @StateId("values")
    private final StateSpec<BagState<String>> valuesState = StateSpecs.bag();
    @SuppressWarnings("unused")
    @StateId("timerSet")
    private final StateSpec<ValueState<Boolean>> timerSetState = StateSpecs.value();
    @SuppressWarnings("unused")
    @TimerId("expire")
    private final TimerSpec expireTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @SuppressWarnings("unused")
    @StartBundle
    public void startBundle() {
      LOG.info("Print. Start bundle");
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window,
        @StateId("key") ValueState<String> keyState,
        @StateId("values") BagState<String> valuesState,
        @AlwaysFetched @StateId("timerSet") ValueState<Boolean> timerSetState,
        @TimerId("expire") Timer expireTimer,
        PipelineOptions options) {

      String key = context.element().getKey();
      String value = context.element().getValue();
      Instant timestamp = context.timestamp();

      LOG.info("Print.processElement. key: {}, timestamp: {}, window: {}, value: {}",
          key, timestamp, window, value);

      keyState.write(key);
      valuesState.add(value);
      boolean timerSet = Optional.ofNullable(timerSetState.read()).orElse(false);
      if (!timerSet) {
        expireTimer.set(window.maxTimestamp());
        timerSetState.write(true);
      }
    }

    @SuppressWarnings("unused")
    @OnTimer("expire")
    public void onExpireTimer(OnTimerContext context,
        @AlwaysFetched @StateId("key") ValueState<String> keyState,
        @AlwaysFetched @StateId("values") BagState<String> valuesState) {
      Instant timestamp = context.timestamp();
      BoundedWindow window = context.window();

      String key = keyState.read();
      Iterable<String> values = valuesState.read();

      LOG.info("Print.onExpireTimer. key: {}, timestamp: {}, window: {}, values: {}", key,
          timestamp, window, values);
    }

    @SuppressWarnings("unused")
    @FinishBundle
    public void finishBundle() {
      LOG.info("Print. End bundle");
    }

  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(PrintPipelineOptions.class);
    PrintPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        .as(PrintPipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read", new PTransform<PBegin, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PBegin input) {
            switch (options.getSourceMode()) {
              case BOUNDED:
                return input.apply("BoundedRead",
                    Read.from(
                        new LongBoundedSource(options.getStartElement(), options.getEndElement())));
              case UNBOUNDED:
                return input.apply("UnboundedRead",
                    Read.from(new LongUnboundedSource(options.getStartElement(),
                        options.getEndElement())));
              default:
                throw new IllegalStateException(
                    "Unexpected SourceMode: '" + options.getSourceMode() + "'");
            }
          }
        })
        .apply("ToKV", ParDo.of(new ConvertToKVFn()))
        .apply("Windowing", Window.into(
            FixedWindows.of(Duration.standardSeconds(options.getWindowSeconds()))))
        .apply("Print", new PTransform<PCollection<KV<String, String>>, PDone>() {
          @Override
          public PDone expand(PCollection<KV<String, String>> input) {
            switch (options.getPrintStepMode()) {
              case GBK:
                input
                    .apply("GBK", GroupByKey.create())
                    .apply("NonStatefulPrint", ParDo.of(new NonStatefulPrintFn()));
                break;
              case STATEFUL:
                input
                    .apply("StatefulPrint", ParDo.of(new StatefulPrintFn()));
                break;
              default:
                throw new IllegalStateException(
                    "Unexpected PrintStepMode: '" + options.getPrintStepMode() + "'");
            }
            return PDone.in(input.getPipeline());
          }
        });

    pipeline.run();
  }
}
