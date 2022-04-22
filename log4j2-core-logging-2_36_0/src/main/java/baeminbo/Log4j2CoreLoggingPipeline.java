package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Log4j2CoreLoggingPipeline {

  public static class Log4jLoggingDoFn extends DoFn<Long, Void> {
    // Follow the example in https://logging.apache.org/log4j/2.x/manual/configuration.html
    private static final org.apache.logging.log4j.Logger LOG4J_LOG = org.apache.logging.log4j.LogManager.getLogger(
        Log4jLoggingDoFn.class);

    @ProcessElement
    public void processElement(ProcessContext context) throws InterruptedException {
      long element = context.element();
      LOG4J_LOG.trace("LOG4j_LOG. TRACE. element:{}", element);
      LOG4J_LOG.debug("LOG4J_LOG. DEBUG. element:{}", element);
      LOG4J_LOG.info("LOG4J_LOG. INFO. element:{}", element);
      LOG4J_LOG.warn("LOG4j_LOG. WARN. element:{}", element);
      LOG4J_LOG.error("LOG4J_LOG. ERROR. element:{}", element);
      LOG4J_LOG.fatal("LOG4J_LOG. FATAL. element:{}", element);
      Thread.sleep(
          10000); // Sleep 10 seconds for logs in local file being updated to Cloud Logging.
    }
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("Read", GenerateSequence.from(0).to(10))
        .apply("Log", ParDo.of(new Log4jLoggingDoFn()));

    pipeline.run();
  }
}
