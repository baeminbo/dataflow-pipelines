package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeftOnlyFlattenUnzippedSideInputPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(
      LeftOnlyFlattenUnzippedSideInputPipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);
    PCollectionView<String> sideView = pipeline
        .apply("Side", Create.of("__SIDE__"))
        .apply("PrintSide", ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            String element = context.element();
            LOG.info("PrintSide; element:{}", element);
            context.output(element);
          }
        }))
        .apply(View.asSingleton());

    PCollection<String> main = pipeline
        .apply("Main",
            Read.from(new SimpleCountingSource(1000, 2000, Duration.standardSeconds(1))))
        .apply("WindowingBy15s", Window.into(FixedWindows.of(Duration.standardSeconds(15))))
        .apply("PrintMain", ParDo.of(new DoFn<Long, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Long element = context.element();
            LOG.info("PrintMain. element:{}", element);
            context.output(Long.toString(element));
          }
        }));

    PCollection<String> left = main.apply("Left", ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window) {
        String element = context.element();
        Instant timestamp = context.timestamp();
        String outputElement = "Left-" + element;
        LOG.info("LeftOnly; {} -> {}, timestamp:{}, window:{}", element, outputElement, timestamp,
            window);
        context.output(outputElement);
      }
    }));

    PCollection<String> right = main.apply("DiscardRight", ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window) {
        String element = context.element();
        Instant timestamp = context.timestamp();
        LOG.info("DiscardRight; element:{}, timestamp:{}, window:{}", element, timestamp, window);
      }
    }));

    PCollectionList.of(left).and(right)
        .apply(Flatten.pCollections())
        .apply("Process", ParDo.of(new DoFn<String, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context, BoundedWindow window) {
            String element = context.element();
            Instant timestamp = context.timestamp();
            String side = context.sideInput(sideView);
            LOG.info("Process; element:{}, side:{}, timestamp:{}, window:{}", element, side,
                timestamp, window);
          }
        }).withSideInputs(sideView));

    pipeline.run();
  }
}
