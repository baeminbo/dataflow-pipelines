package baeminbo;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeSideInputPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(LargeSideInputPipeline.class);

  private static final long NUM_SIDE_INPUT_ELEMENTS = 10_000_000;
  private static final long NUM_MAIN_INPUT_ELEMENTS = 1_000;

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    PCollectionView<Map<Long, String>> view = pipeline
        .apply("SideInputCreate", Create.of((Void) null))
        .apply("SideInputFlatMap",
            FlatMapElements.via(new SimpleFunction<Void, Iterable<KV<Long, String>>>() {
              @Override
              public Iterable<KV<Long, String>> apply(Void ignored) {
                return () -> LongStream.range(0, NUM_SIDE_INPUT_ELEMENTS)
                    .mapToObj(i -> KV.of(i, "v" + i)).iterator();
              }
            }))
        .apply("SideInputMapView", View.asMap());

    pipeline
        .apply("MainInputCreate", Create.of((Void) null))
        .apply("MainInputFlatMap",
            FlatMapElements.via(new SimpleFunction<Void, Iterable<Long>>() {
              @Override
              public Iterable<Long> apply(Void input) {
                return () -> LongStream.range(0, NUM_MAIN_INPUT_ELEMENTS).iterator();
              }
            }))
        .apply(Reshuffle.viaRandomKey())
        .apply("Print", ParDo.of(new DoFn<Long, Void>() {
          Map<Long, String> sideMap = null;
          @ProcessElement
          public void process(ProcessContext context, BoundedWindow window) {
            Long element = context.element();
            if (sideMap == null) {
              sideMap = new HashMap<>(context.sideInput(view));
            }
            String sideValue = sideMap.get(element);
            LOG.info("print. element:{}, sideValue:{}", element, sideValue);
          }
        }).withSideInputs(view));
    pipeline.run();
  }
}
