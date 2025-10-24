package baeminbo;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;

public class TlsPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(TlsPipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(Create.of(1, 2, 3, 4, 5, 6))
      .apply(Reshuffle.viaRandomKey())
      .apply(ParDo.of(new DoFn<Integer, Void>() {
        @ProcessElement
        public void processElement(ProcessContext context) throws InterruptedException {
          Integer element = context.element();
          LOG.info("Processing element " + element);
          Thread.sleep(20000); // waiting for log sent to Cloud Logging
        }
      }));

    pipeline.run();
  }

  @AutoService(JvmInitializer.class)
  public static class TlsInitializer implements JvmInitializer {
    @Override
    public void onStartup() {
      System.setProperty("javax.net.debug", "ssl:handshake");
//      Disabled GCM may cause handshake failure with
      Security.setProperty("jdk.tls.disabledAlgorithms", "GCM");
      System.out.println("Disabled algorithms: " + Security.getProperty("jdk.tls.disabledAlgorithms"));
    }
  }
}
