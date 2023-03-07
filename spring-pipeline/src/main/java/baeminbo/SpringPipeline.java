package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;

public class SpringPipeline {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new ProcessDoFn<>()));

    pipeline.run();
  }
}
