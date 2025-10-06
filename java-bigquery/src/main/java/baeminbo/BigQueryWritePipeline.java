package baeminbo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

@SuppressWarnings("nulless")
public class BigQueryWritePipeline {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryWritePipeline.class);

  // Change to DEFAULT_VALUE or NULL_VALUE to test different behaviors for missing values.
  private static final AppendRowsRequest.MissingValueInterpretation MISSING_VALUE_INTERPRETATION =
      AppendRowsRequest.MissingValueInterpretation.MISSING_VALUE_INTERPRETATION_UNSPECIFIED;

  public static TableReference getTableReference(String tableName) {
    return new TableReference().setProjectId("baeminbo-2021").setDatasetId("dataset_20250506").setTableId(tableName);
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    // Table names to insert rows
    PCollection<String> tableNames = pipeline.apply(Create.of("table1", "table2", "table3"));

    BigQueryIO.Write<String> bigQueryWrite = BigQueryIO.<String>write()
        .to(new DynamicDestinations<String, String>() {

          @Override
          public String getDestination(@Nullable ValueInSingleWindow<String> tableName) {
            return Optional.ofNullable(tableName.getValue()).orElse("");
          }

          @NotNull
          @Override
          public TableDestination getTable(String destination) {
            return new TableDestination(getTableReference(destination), null);
          }

          @Override
          public TableSchema getSchema(String destination) {
            TableSchema schema;
            switch (destination) {
              case "table1":
                schema = new TableSchema().setFields(
                    Arrays.asList(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema().setName("value").setType("INT64").setMode("REQUIRED")));
                break;
              case "table2":
                schema = new TableSchema().setFields(
                    Arrays.asList(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema().setName("value")
                            .setType("INT64")
                            .setDefaultValueExpression("2")));
                break;
              case "table3":
                schema = new TableSchema().setFields(
                    Arrays.asList(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema().setName("value")
                            .setType("INT64")
                            .setMode("REQUIRED")
                            .setDefaultValueExpression("3")));
                break;
              default:
                throw new RuntimeException("Unknown table destination: " + destination);
            }

            LOG.info("Schema for {}: {}", destination, schema, new RuntimeException());
            return schema;
          }
        })
        .withFormatFunction((SerializableFunction<String, TableRow>) tableName -> {
          TableRow row = new TableRow();
          row.set("key", MISSING_VALUE_INTERPRETATION+"-with-null-value"+":3");
          row.set("value", null);
          LOG.info("Row for {}: {}", tableName, row, new RuntimeException());
          return row;
        })
        .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE);

    switch (MISSING_VALUE_INTERPRETATION) {
      case MISSING_VALUE_INTERPRETATION_UNSPECIFIED:
        // Do nothing
        break;
      case DEFAULT_VALUE:
      case NULL_VALUE:
        bigQueryWrite = bigQueryWrite.withDefaultMissingValueInterpretation(MISSING_VALUE_INTERPRETATION);
        break;
      default:
        throw new RuntimeException("Unknown value interpretation: " + MISSING_VALUE_INTERPRETATION);
    }

    WriteResult writeResult = tableNames
        .apply("Write",
            bigQueryWrite);

    writeResult.getFailedStorageApiInserts()
        .apply(ParDo.of(new DoFn<BigQueryStorageApiInsertError, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            LOG.info("Failed: {}", c.element());
          }
        }));

    pipeline.run();
  }
}
