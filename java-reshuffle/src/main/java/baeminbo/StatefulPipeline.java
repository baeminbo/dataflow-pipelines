package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class StatefulPipeline {

  public static final int ELEMENT_COUNT = 1_000_000;
  public static final int SHARD_COUNT = 10_000;
  public static final Duration PROCESS_TIME_PER_KV = Duration.ofSeconds(5);
  private static final Logger LOG = LoggerFactory.getLogger(StatefulPipeline.class);

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(""))
        .apply("Generate", ParDo.of(new DoFn<String, KV<Long, Long>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            LOG.info("Generating {} elements to {} shards.", ELEMENT_COUNT, SHARD_COUNT);
            for (long i = 0; i < ELEMENT_COUNT; ++i) {
              c.output(KV.of(i % SHARD_COUNT, i));
            }
            LOG.info("Generated {} elements to {} shards.", ELEMENT_COUNT, SHARD_COUNT);
          }
        }))
        .apply("Process", ParDo.of(new DoFn<KV<Long, Long>, Void>() {
          @StateId("count")
          private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

          @ProcessElement
          public void processElement(ProcessContext c,
                                     @StateId("count") ValueState<Integer> countState) {
            KV<Long, Long> element = c.element();
            LOG.info("Processing: {}", element);
            Instant loopEndTime = Instant.now().plus(PROCESS_TIME_PER_KV);
            while (Instant.now().isBefore(loopEndTime)) {
              // do nothing
            }
            int count = Optional.ofNullable(countState.read()).orElse(0) + 1;
            LOG.info("Processed: {}, count: {}", element, count);
          }
        }));
    pipeline.run();
  }
}
