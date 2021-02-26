import com.google.api.Http;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProxyPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(HttpProxyPipeline.class);

  private static final String URL = "https://example.com";
  private static final String PROXY_HOST = <PROXY_HOST> // e.g. your squid proxy host
  private static final int PROXY_PORT = 3128; // 3128 is the default port for Squid proxy
  private static final String PROXY_SCHME = "http";


  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(GenerateSequence.from(0)) // To make streaming job
        .apply(ParDo.of(new DoFn<Long, Void>() {
          CloseableHttpClient httpClient;

          @Setup
          public void setUp() {
            httpClient = HttpClients.custom().build();
          }

          @Teardown
          public void tearDown() throws IOException {
            if (httpClient != null) {
              httpClient.close();
            }
          }

          @ProcessElement
          public void processElement(ProcessContext context) throws IOException {
            long element = context.element();

            HttpGet httpGet = new HttpGet(URL);
            httpGet.setConfig(RequestConfig.custom()
                .setProxy(new HttpHost(PROXY_HOST, PROXY_PORT, PROXY_SCHME))
                .build());

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
              StatusLine statusLine = response.getStatusLine();
              LOG.info("Get #{}. status: {}", element, statusLine);
            }
          }
        }));

    pipeline.run();
  }
}
