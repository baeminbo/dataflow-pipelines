package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class GroupIntoBatchesPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(GroupIntoBatchesPipeline.class);
  public static final int ELEMENT_COUNT = 1_000_000;
  public static final int SHARD_COUNT = 10_000;
  public static final int BATCH_SIZE = 10;
  public static final Duration PROCESS_TIME_PER_KV = Duration.ofSeconds(5);

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
        .apply(GroupIntoBatches.ofSize(BATCH_SIZE))
        .apply("Process", ParDo.of(new DoFn<KV<Long, Iterable<Long>>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            KV<Long, Iterable<Long>> element = c.element();
            LOG.info("Processing: {}", element);
            Instant loopEndTime = Instant.now().plus(PROCESS_TIME_PER_KV);
            while (Instant.now().isBefore(loopEndTime)) {
              // do nothing
            }
            LOG.info("Processed: {}", element);
          }
        }));
    pipeline.run();
  }
}
