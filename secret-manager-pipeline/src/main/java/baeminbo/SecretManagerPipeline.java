package baeminbo;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretManagerPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerPipeline.class);

  public interface RunOptions extends PipelineOptions {

    @Description(
        "Secret version name. e.g. \"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{VERSION}\""
            + "See https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions/access")
    @Required
    ValueProvider<String> getSecretVersionName();
    void setSecretVersionName(ValueProvider<String> secretVersionName);
  }

  private static String getSecret(String secretVersionName) {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse response = client
          .accessSecretVersion(SecretVersionName.parse(secretVersionName));
      return response.getPayload().getData().toStringUtf8();
    } catch (IOException e) {
      throw new RuntimeException("Cannot get secret: " + secretVersionName, e);
    }
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(RunOptions.class);
    RunOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create()
        .as(RunOptions.class);

    ValueProvider<String> secret = NestedValueProvider.of(options.getSecretVersionName(),
        (SerializableFunction<String, String>) SecretManagerPipeline::getSecret);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(GenerateSequence.from(0).to(10))
        .apply("CallWithSecret", ParDo.of(new DoFn<Long, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            long element = context.element();
            // This is just a example to show how to use SecretManager in Dataflow.
            // You must not print secret in logs.
            LOG.info("Call with parameter: {} and secret: {}", element, secret.get());
          }
        }));

    pipeline.run();
  }
}
