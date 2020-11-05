package baeminbo;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


  public class HotspotGbkPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(HotspotGbkPipeline.class);

  private static ResourceId getWorkEverFailed(PipelineOptions options) {
    return FileSystems.matchNewResource(options.getTempLocation(), true)
        .resolve(options.getJobName(), StandardResolveOptions.RESOLVE_DIRECTORY)
        .resolve("work-failed", StandardResolveOptions.RESOLVE_FILE);
  }

  private static boolean failIfNeverFailed(PipelineOptions options) throws IOException {
    ResourceId failMarkFile = getWorkEverFailed(options);

    MatchResult match = FileSystems.match(failMarkFile.toString());
    if (match.status() == Status.OK) {
      LOG.info("Once failed already. File exists: {}", failMarkFile);
      return false;
    } else {
      LOG.info("Never failed yet. status: {}. Creating file: {}", match.status(), failMarkFile);
      try (PrintWriter writer = new PrintWriter(Channels.newOutputStream(
          FileSystems.create(failMarkFile, "text/plain")))) {
        writer.println("hello world");
      }
      LOG.info("File created: {}", failMarkFile);
      return true;
    }
  }

  @SuppressWarnings("unused")
  public interface Options extends PipelineOptions {

    @Description("Start element (inclusive")
    long getStartElement();

    void setStartElement(long startElement);

    @Description("End element (exclusive")
    long getEndElement();

    void setEndElement(long endElement);

    @Description("The number of shards")
    long getShardCount();

    void setShardCount(long shardCount);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    long startElement = options.getStartElement();
    long endElement = options.getEndElement();
    long shardCount = options.getShardCount();

    pipeline
        .apply(GenerateSequence.from(startElement).to(endElement))
        .apply(WithKeys
            .of((SerializableFunction<Long, String>) element ->
                Long.toString(Math.floorMod(element, shardCount)))
            .withKeyType(new TypeDescriptor<String>() {
            }))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new DoFn<KV<String, Iterable<Long>>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context) throws IOException {
            if (failIfNeverFailed(context.getPipelineOptions())) {
              throw new RuntimeException("Fail deliberately");
            }
            String key = context.element().getKey();
            Iterable<Long> values = context.element().getValue();

            long minValue = Long.MAX_VALUE;
            long maxValue = Long.MIN_VALUE;
            long valueCount = 0;
            for (long value : values) {
              minValue = Math.min(minValue, value);
              maxValue = Math.max(maxValue, value);
              ++valueCount;
            }

            if (valueCount > 0) {
              LOG.info("Print. key: {}, valueCount: {}, minValue: {}, maxValue:{}", key, valueCount,
                  minValue, maxValue);
            } else {
              LOG.info("Print. key: {}, no values", key);
            }
          }
        }));

    pipeline.run();
  }
}
