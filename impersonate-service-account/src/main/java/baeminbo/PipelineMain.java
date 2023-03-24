package baeminbo;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineMain {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineMain.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    String project = options.as(GcpOptions.class).getProject();
    String table = String.format("%s.dataset1.table1", project);

    pipeline
        .apply(BigQueryIO.readTableRows()
            .fromQuery(String.format("SELECT * FROM `%s`", table))
            .usingStandardSql()
            // Disable validation to avoid permission error at job creation before the job is
            // submitted to Dataflow.
            .withoutValidation())
        .apply(ParDo.of(new DoFn<TableRow, Void>() {
          @ProcessElement
          public void processElement(@Element TableRow row) {
            LOG.info("row: {}", row);
          }
        }));

    pipeline.run();
  }
}
