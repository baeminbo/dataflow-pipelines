package baeminbo;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupValuesPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(GroupValuesPipeline.class);

  private static final long ELEMENT_COUNT = 3000;
  private static final long SHARD_COUNT = 10;
  private static final Duration SOURCE_SLEEP = Duration.millis(100);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Read.from(CreateStreamingSource.of(ELEMENT_COUNT, SOURCE_SLEEP)))
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
            .via(i -> KV.of(Long.toString(i % SHARD_COUNT), i)))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(GroupByKey.create())
        .apply(Combine.groupedValues(new CombineFn<Long, List<Long>, Long>() {
          // Get sum of all the inputs
          @Override
          public List<Long> createAccumulator() {
            LOG.info("create accumulator.");
            return new ArrayList<>();
          }

          @Override
          public List<Long> addInput(List<Long> acc, Long input) {
            LOG.info("add input:{}", input);
            acc.add(input);
            return acc;
          }

          @Override
          public List<Long> mergeAccumulators(Iterable<List<Long>> accs) {
            LOG.info("merge accumulators:{}", accs);
            List<Long> newAcc = new ArrayList<>();
            for (List<Long> acc : accs) {
              newAcc.addAll(acc);
            }
            return newAcc;
          }

          @Override
          public Long extractOutput(List<Long> acc) {
            LOG.info("extract output for:{}", acc);
            long sum = 0;
            for (long i : acc) {
              sum += i;
            }
            return sum;
          }
        }))
        .apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context, BoundedWindow window) {
            LOG.info("print. element:{} timestamp:{} window:{} pane:{}", context.element(),
                context.timestamp(), window, context.pane());
          }
        }));

    pipeline.run();
  }
}
