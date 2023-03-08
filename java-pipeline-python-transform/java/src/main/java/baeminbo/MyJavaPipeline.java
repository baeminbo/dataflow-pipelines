package baeminbo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class MyJavaPipeline {
  private static final String PYTHON_TRANSFORM = "mytransforms.Printer";

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    List<String> extraPackages = options.as(Options.class).getPythonExtraPackages();

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(Create.of(LongStream.range(100, 200).boxed().collect(Collectors.toList())))
        .apply(PythonExternalTransform
            .<PCollection<Long>, PCollection<Void>>from(PYTHON_TRANSFORM)
            .withKwarg("name", "MyPrinter")
            .withExtraPackages(extraPackages));

    pipeline.run();
  }

  public interface Options extends PipelineOptions {

    @Description("Extra Python packages to launch expansion service and stage for Python SDK harness container.")
    List<String> getPythonExtraPackages();

    void setPythonExtraPackages(List<String> extraPackages);
  }
}
