package baeminbo;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryPermissionPipelineMain {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryPermissionPipelineMain.class);
  public static void main(String[] args) {
    PipelineMainOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineMainOptions.class);

    ValueProvider<String> query = options.getQuery();
    if (query.isAccessible()) {
      LOG.info("Query is: {}", query.get());
    } else {
      LOG.info("No query is set. If this is creating a Dataflow template, the query must be set at "
          + "a template job run");
    }

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(BigQueryIO.readTableRows().fromQuery(query).usingStandardSql().withoutValidation())
        .apply(ParDo.of(new DoFn<TableRow, Void>() {
          @ProcessElement
          public void processElement(@Element TableRow row) {
            LOG.info("row: {}", row);
          }
        }));

    pipeline.run();
  }

  public interface PipelineMainOptions extends PipelineOptions {
    @Description("BigQuery query to run.")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);
  }
}
