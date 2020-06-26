package baeminbo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadOOMPipeline {

  private static final Logger log = LoggerFactory.getLogger(ThreadOOMPipeline.class);

  private static long RANDOM_SLEEP = Duration.standardMinutes(5).getMillis();

  public interface Options extends PipelineOptions {

    @Description("Pub/Sub subscription. e.g. projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION>")
    @Required
    ValueProvider<String> getSubscription();

    void setSubscription(ValueProvider<String> subscription);

    @Description("Pub/sub topic. e.g. projects/<PROJECT_ID>/topics/<TOPIC>")
    @Required
    ValueProvider<String> getTopic();

    void setTopic(ValueProvider<String> topic);
  }

  private static List<String> processInParallel(Iterable<String> input) {
    ExecutorService executor = Executors.newFixedThreadPool(1000);
    try {

      List<Future<String>> futures = StreamSupport
          .stream(input.spliterator(), true)
          .map(i ->
              executor.submit(() -> {
                    Random random = ThreadLocalRandom.current();
                    long sleepMs = Math.abs(random.nextLong() % RANDOM_SLEEP);
                    Thread.sleep(sleepMs);

                    log.info("Processed input: {}, process time: {}", i, Duration.millis(sleepMs));
                    return "processed-" + i;
                  }
              ))
          .collect(Collectors.toList());

      List<String> output = new ArrayList<>(futures.size());
      futures.forEach(future -> {
        try {
          output.add(future.get());
        } catch (Exception e) {
          log.error("Cannot process. input: {}", input, e);
          throw new RuntimeException("Cannot process input");
        }
      });
      return output;
    } finally {
      executor.shutdown();
    }
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(PubsubIO.readStrings().fromSubscription(options.getSubscription()))
        .apply("Split", ParDo.of(new DoFn<String, KV<String, String>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            for (String word : context.element().split("\\s+")) {
              try {
                String[] kv = word.split(":", 2);
                context.output(KV.of(kv[0], kv[1]));
              } catch (RuntimeException e) {
                log.warn("Stop splitting due to invalid word format encountered. word: [{}]", word);
                return;
              }
            }
          }
        }))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(GroupByKey.create())
        .apply("InParallel", ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Iterable<String> input = context.element().getValue();
            List<String> output = processInParallel(input);
            for (String s : output) {
              context.output(s);
            }
          }
        }))
        .apply(PubsubIO.writeStrings().to(options.getTopic()));

    pipeline.run();
  }
}
