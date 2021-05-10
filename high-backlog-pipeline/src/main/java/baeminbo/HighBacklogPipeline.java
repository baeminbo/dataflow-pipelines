package baeminbo;

import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HighBacklogPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(HighBacklogPipeline.class);

  private static final long SHARD_COUNT = 10;

  public interface HighBacklogOptions extends PipelineOptions {

    @Description("Pub/Sub subscription to pull messages from")
    @Required
    String getSubscription();

    void setSubscription(String subscription);

    @Description("Sleep time in the step before GroupByKey. By default, zero which means no sleep")
    @Default.Long(0L)
    Long getBeforeSleepMillis();

    void setBeforeSleepMillis(Long beforeSleepMillis);

    @Description("Sleep time in the step after GroupByKey. By default, zero which means no sleep")
    @Default.Long(0L)
    Long getAfterSleepMillis();

    void setAfterSleepMillis(Long afterSleepMillis);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(HighBacklogOptions.class);

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(PubsubIO.readMessagesWithAttributesAndMessageId()
            .fromSubscription(options.as(HighBacklogOptions.class).getSubscription()))
        .apply("Before", ParDo.of(new DoFn<PubsubMessage, PubsubMessage>() {
          @ProcessElement
          public void processElement(ProcessContext context) throws InterruptedException {
            long sleepMillis = context.getPipelineOptions().as(HighBacklogOptions.class)
                .getBeforeSleepMillis();
            if (sleepMillis > 0) {
              Thread.sleep(sleepMillis);
            }
            context.output(context.element());
          }
        }))
        .apply("ToKV", MapElements.into(
            TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptor.of(PubsubMessage.class)))
            .via(message -> KV
                .of(Math.floorMod(Long.parseLong(Objects.requireNonNull(message.getMessageId())),
                    SHARD_COUNT), message)))
        .apply(Reshuffle.of())
        .apply("After", ParDo.of(new DoFn<KV<Long, PubsubMessage>, KV<Long, PubsubMessage>>() {
          @ProcessElement
          public void processElement(ProcessContext context) throws InterruptedException {
            long sleepMillis = context.getPipelineOptions().as(HighBacklogOptions.class)
                .getAfterSleepMillis();
            if (sleepMillis > 0) {
              Thread.sleep(sleepMillis);
            }
            context.output(context.element());
          }
        }))
        .apply("Print", ParDo.of(new DoFn<KV<Long, PubsubMessage>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context, BoundedWindow window) {
            PubsubMessage message = context.element().getValue();
            LOG.info("Print. messageId:{}, timestamp:{}, window:{}",
                message.getMessageId(), context.timestamp(), window);
          }
        }));

    pipeline.run();
  }
}
