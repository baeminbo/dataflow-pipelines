package baeminbo;

import java.net.URI;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZstdOutputPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(ZstdOutputPipeline.class);
  public interface ZstdOutputOptions extends PipelineOptions {
    @Description("The location of compressed output. If not specified, "
        + "<tempLocation>/<randomUUID>/output is used instead, where tempLocation is the value of "
        + "--tempLocation")
    String getOutput();

    void setOutput(String output);

    @Description("Get compression mode. Default is ZSTD.")
    @Default.Enum("ZSTD")
    Compression getCompression();

    void setCompression(Compression compression);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ZstdOutputOptions.class);

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    ZstdOutputOptions zstdOutputOptions = options.as(ZstdOutputOptions.class);
    String output = zstdOutputOptions.getOutput();
    if (output == null) {
      String tempLocation = options.as(DataflowPipelineOptions.class).getTempLocation();
      output = URI.create(tempLocation + "/" + UUID.randomUUID() + "/output").normalize().toASCIIString();
      LOG.warn("`--output` is not specified in command options. `{}` is used instead.", output);
    } else {
      LOG.info("Output location is `{}`.", output);
    }
    Compression compression = zstdOutputOptions.getCompression();
    LOG.info("Compression mode is '{}'", compression);

    pipeline
        .apply(Create.of("Hello, world", "Good morning"))
        .apply(TextIO.write().withCompression(compression).to(output));

    pipeline.run();
  }
}
