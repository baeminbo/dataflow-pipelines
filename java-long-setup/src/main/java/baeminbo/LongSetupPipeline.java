package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class LongSetupPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(LongSetupPipeline.class);

  private static final long ELEMENT_COUNT = 100;

  private static final Duration SLEEP_DURATION = Duration.ofMinutes(4);

  private static final Duration PROCESS_ELEMENT_LOOP_DURATION = Duration.ofSeconds(5);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(GenerateSequence.from(0).to(ELEMENT_COUNT))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new LongSetupDoFn(SLEEP_DURATION)));

    pipeline.run();
  }

  public static class LongSetupDoFn extends DoFn<Long, Void> {
    private final Duration sleepDuration;

    public LongSetupDoFn(Duration sleepDuration) {
      this.sleepDuration = sleepDuration;
    }

    @Setup
    public void setup() throws InterruptedException {
      LOG.info("[setup]({}) sleep for {}", System.identityHashCode(this), sleepDuration, new RuntimeException("for stacktrace"));
      Thread.sleep(sleepDuration.toMillis());
      LOG.info("[setup]({}) end", System.identityHashCode(this));
    }

    @Teardown
    public void teardown() {
      LOG.info("[teardown]({})", System.identityHashCode(this), new RuntimeException("for stacktrace"));
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
      LOG.info("[startBundle]({})", System.identityHashCode(this));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      LOG.info("[finishBundle]({})", System.identityHashCode(this));
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Long element = c.element();
      LOG.info("[process]({}) sleep for {}, element: {}", System.identityHashCode(this), PROCESS_ELEMENT_LOOP_DURATION, element);
      Instant loopEndTime = Instant.now().plus(PROCESS_ELEMENT_LOOP_DURATION);
      while (Instant.now().isBefore(loopEndTime)) {
        // just loop for element process delay
      }
      LOG.info("[process]({}) end element: {}", System.identityHashCode(this), element);
    }
  }
}
