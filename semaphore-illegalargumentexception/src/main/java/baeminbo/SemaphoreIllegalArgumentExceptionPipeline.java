package baeminbo;

import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SemaphoreIllegalArgumentExceptionPipeline {

  private static final Logger log = LoggerFactory
      .getLogger(SemaphoreIllegalArgumentExceptionPipeline.class);

  private static final int OUTPUT_BYTES = 64 * 1024 * 1024;

  public interface Options extends PipelineOptions {

    @Description("Pub/Sub subscription. e.g. projects/<PROJECT>/subscription/<SUBSCRIPTION_ID>")
    @Required
    ValueProvider<String> getSubscription();

    void setSubscription(ValueProvider<String> subscription);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<Void, byte[]>> items = pipeline
        .apply(PubsubIO.readMessagesWithAttributesAndMessageId()
            .fromSubscription(options.as(Options.class).getSubscription()))
        .apply("ToKV",
            MapElements.via(new SimpleFunction<PubsubMessage, KV<Void, PubsubMessage>>() {
              @Override
              public KV<Void, PubsubMessage> apply(PubsubMessage input) {
                return KV.of(null, input);
              }
            }))
        .apply(Window.<KV<Void, PubsubMessage>>into(new GlobalWindows())
            .triggering(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane()
                        .alignedTo(Duration.standardSeconds(10))))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new DoFn<KV<Void, Iterable<PubsubMessage>>, KV<Void, byte[]>>() {
          @StateId("index")
          private final StateSpec<ValueState<Integer>> index = StateSpecs.value();

          @ProcessElement
          public void processElement(ProcessContext context, BoundedWindow window,
              @StateId("index") ValueState<Integer> index) {

            int currentIndex = Optional.ofNullable(index.read()).orElse(0);
            int outputCount = currentIndex + 1;
            for (int i = 0; i < outputCount; ++i) {
              context.output(KV.of(null, new byte[OUTPUT_BYTES]));
            }
            log.info("output. currentIndex: {}, totalBytes: {}", currentIndex,
                outputCount * OUTPUT_BYTES);

            index.write(currentIndex + 1);
          }
        }));

    for (int i = 1; i <= 4; ++i) {
      Integer index = i;
      items
          .apply("FixedWindow_" + i + "m",
              Window.into(FixedWindows.of(Duration.standardMinutes(i))))
          .apply("GroupByKey_" + i, GroupByKey.create())
          .apply("Print_" + i, ParDo.of(new DoFn<KV<Void, Iterable<byte[]>>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
              int count = 0;
              long totalBytes = 0;
              for (byte[] v : context.element().getValue()) {
                ++count;
                totalBytes += v.length;
              }
              log.info("Print_{}. count: {}, totalBytes: {}", index, count, totalBytes);
            }
          }));
    }

    pipeline.run();
  }
}
