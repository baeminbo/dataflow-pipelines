package baeminbo;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class BigQueryReadHotkeyMain {

  private static final String DEFAULT_TABLE = "bigquery-public-data.samples.wikipedia";

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    JobOptions jobOptions = options.as(JobOptions.class);
    ValueProvider<String> table = jobOptions.getInputTable();
    ValueProvider<String> output = jobOptions.getOutput();

    pipeline
        // Disable validation as it's not supported for a table set dynamically through
        // ValueProvider.
        .apply("ReadBigQuery", BigQueryIO.readTableRows().from(table).withoutValidation())
        .apply("RowToString", ParDo.of(new DoFn<TableRow, String>() {
          @ProcessElement
          public void processElement(@Element TableRow row, OutputReceiver<String> outputReceiver)
              throws IOException {
            outputReceiver.output(GsonFactory.getDefaultInstance().toString(row));
          }
        }))
        .apply("WriteToGcs", TextIO.write().to(output).withoutSharding());
    pipeline.run();
  }

  public interface JobOptions extends PipelineOptions {
    @Description("BigQuery table to read. The pipeline reads the whole data.")
    @Default.String(DEFAULT_TABLE)
    ValueProvider<String> getInputTable();

    void setInputTable(ValueProvider<String> inputTable);

    // Can be set dynamically in Dataflow template run.
    @Description("GCS location to write the output.")
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> output);
  }
}
