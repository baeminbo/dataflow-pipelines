package baeminbo;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySetFPipelineFixed {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySetFPipelineFixed.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    WriteResult write = pipeline.apply(Create.of(""))
      .apply(ParDo.of(new DoFn<String, Integer>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(1_000);
          c.output(1_000_000);
          c.output(10_000_000);
        }
      }))
      .apply(MapElements.via(new SimpleFunction<Integer, TableRow>() {
        @Override
        public TableRow apply(Integer bytesSize) {
          TableRow row = new TableRow();
          row.setF(ImmutableList.of(
            new TableCell().setV(bytesSize),
            new TableCell().setV(new byte[bytesSize])));
          return row;
        }
      }))
      .apply(BigQueryIO.writeTableRows()
        .to("<REDACTED>")
        .optimizedWrites()
        .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        .withFormatRecordOnFailureFunction(new SimpleFunction<TableRow, TableRow>() {
          @Override
          public TableRow apply(TableRow row) {
            return row;
          }
        }));


    write.getFailedStorageApiInserts()
      .apply(ParDo.of(new DoFn<BigQueryStorageApiInsertError, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          TableRow row = c.element().getRow();
          String errorMessage = c.element().getErrorMessage();
          LOG.info("Failed to insert.\nrow:{}\nerror:{}", row, errorMessage);
        }
      }));

    pipeline.run();
  }
}
