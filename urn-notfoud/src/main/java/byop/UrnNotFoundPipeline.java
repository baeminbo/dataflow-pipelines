package byop;

import static org.apache.beam.sdk.transforms.Watch.Growth.never;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UrnNotFoundPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(UrnNotFoundPipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(FileIO.match()
            .filepattern("gs://apache-beam-samples/shakespeare/*")
            .continuously(Duration.standardMinutes(1), never())
        )
        .apply(FileIO.readMatches())
        .apply(ParDo.of(new DoFn<ReadableFile, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) throws IOException {
            ReadableFile file = context.element();
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Channels.newInputStream(file.open())))) {
              reader.lines()
                  .flatMap(s -> Arrays.stream(s.split("[^\\p{L}]+")))
                  .forEach(context::output);
            }
          }
        }))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(Count.perElement())
        .apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context, BoundedWindow window) {
            LOG.info("[{}] {}: {}", window, context.element().getKey(),
                context.element().getValue());
          }
        }));

    pipeline.run();
  }
}
