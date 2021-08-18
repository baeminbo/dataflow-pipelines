package baeminbo;

import java.net.URI;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class StuckPipeline {

  private static final long ELEMENTS_PER_INPUT = 1000 * 1000;

  public interface Options extends PipelineOptions {

    @Description("Number of inputs")
    @Default.Integer(1000)
    int getInputs();

    void setInputs(int inputs);

    @Description("Output location. For example, \"gs://<BUCKET>/output-\". Default is \"<tempLocation>/output-\"")
    @Default.String("")
    String getOutputPrefix();

    void setOutputPrefix(String outputPrefix);

    @Description("Number of shards writing to files. By default, decided by the service")
    @Default.Integer(0)
    int getShards();

    void setShards(int shards);
  }

  private static String decideOutputPrefix(PipelineOptions options) {
    String outputPrefix = options.as(Options.class).getOutputPrefix();
    if (outputPrefix != null && !outputPrefix.isEmpty()) {
      return outputPrefix;
    }

    String gcpTempLocation = options.as(GcpOptions.class).getGcpTempLocation();
    if (gcpTempLocation == null || gcpTempLocation.isEmpty()) {
      throw new RuntimeException("Cannot decide `outputPrefix` due to no `gcpTempLocation`");
    }
    return URI.create(gcpTempLocation).resolve("/output").toASCIIString();
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(
            IntStream.range(0, options.getInputs()).boxed().collect(Collectors.toList())))
        .apply(ParDo.of(new DoFn<Integer, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            int input = context.element();
            for (long i = input * ELEMENTS_PER_INPUT; i < (input + 1) * ELEMENTS_PER_INPUT; ++i) {
              context.output(Long.toString(i));
            }
          }
        }))
        .apply(TextIO.write()
            .to(decideOutputPrefix(options))
            .withNumShards(options.getShards()));

    pipeline.run();
  }
}
