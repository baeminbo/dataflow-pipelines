import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Print string element and return it.
 */
public class Print extends PTransform<PCollection<String>, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(Print.class);

  /**
   * A string printed in log.
   *
   * @see PrintDoFn#processElement(ProcessContext, BoundedWindow)
   */
  private final String tag;

  public Print(String tag) {
    this.tag = tag;
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input.apply("Print", ParDo.of(new PrintDoFn()));
  }

  class PrintDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      LOG.info("Print [{}] element: {}, timestamp: {}, window: {}, pane: {}", tag,
          context.element(), context.timestamp(), window, context.pane());
    }
  }

  @AutoService(ExternalTransformRegistrar.class)
  public static class External implements ExternalTransformRegistrar {
    private static final String URN = "beam:external:java:baeminbo:print:v1";
    @Override
    public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
      return Collections.singletonMap(URN, new Builder());
    }

    public static class Configuration {
      private String tag;

      public void setTag(String tag) {
        this.tag = tag;
      }
    }

    static class Builder implements
        ExternalTransformBuilder<Configuration, PCollection<String>, PCollection<String>> {
      @Override
      public PTransform<PCollection<String>, PCollection<String>> buildExternal(
          Configuration configuration) {
        return new Print(configuration.tag);
      }
    }
  }
}
