package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleRunSideinputPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DoubleRunSideinputPipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollectionView<Iterable<Integer>> output1 = pipeline
        .apply("Input1", Create.of(1, 2, 3))
        .apply("Output1", View.asIterable());

    pipeline
        .apply("Input2", Create.of("a", "b", "c"))
        .apply("Output2",
            MapElements.into(TypeDescriptors.voids())
                .via(Contextful.fn((Fn<String, Void>) (element, c) -> {
                  Iterable<Integer> numbers = c.sideInput(output1);
                  LOG.info("main: {}, sideinput: {}", element, numbers);
                  return null;
                }, Requirements.requiresSideInputs(output1))));


    options.setJobName("first-run-sideinput");
    pipeline.run();

    options.setJobName("second-run-sideinput");
    pipeline.run();
  }
}
