package baeminbo;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyJavaPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MyJavaPipeline.class);
  private static final String PYTHON_TRANSFORM = "mytransforms.Printer";

  private static final String DEFAULT_PRINTER_NAME = "MyPrinter";

  private static final List<String> DEFAULT_EXTRA_PACKAGES = Collections.singletonList("mytransforms-1.0.0.tar.gz");

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    List<String> extraPackages = options.as(Options.class).getPythonExtraPackages();
    if (extraPackages != null && !extraPackages.isEmpty()) {
      LOG.info("Using extra packages: {}", extraPackages);
    } else {
      LOG.info("Using default extra packages: {}", DEFAULT_EXTRA_PACKAGES);
      extraPackages = DEFAULT_EXTRA_PACKAGES;
    }

    String printerName = options.as(Options.class).getPrinterName();
    LOG.info("Using printer name: {}", printerName);

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(Create.of(LongStream.range(100, 200).boxed().collect(Collectors.toList())))
        .apply(PythonExternalTransform
            .<PCollection<Long>, PCollection<Void>>from(PYTHON_TRANSFORM)
            .withKwarg("name", printerName)
            .withExtraPackages(extraPackages));

    pipeline.run();
  }

  public interface Options extends PipelineOptions {

    @Description("Extra Python packages to launch expansion service and stage for Python SDK "
        + "harness container. Default is ")
    @Nullable List<String> getPythonExtraPackages();

    void setPythonExtraPackages(List<String> extraPackages);

    @Description("Customize the printer name part in Cloud Logging. For checking if arguments "
        + "passed to Python SDK container properly. Default is " + DEFAULT_PRINTER_NAME)
    @Default.String(DEFAULT_PRINTER_NAME)
    String getPrinterName();

    void setPrinterName(String printerName);
  }
}
