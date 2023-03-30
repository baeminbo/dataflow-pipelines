package baeminbo;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteBigQueryPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBigQueryPipeline.class);

  static PCollection<KV<Void, Long>> createSource(String name, PBegin input, long from) {
    return input
        .apply(String.format("[%s] Generate from %d", name, from), GenerateSequence.from(from))
        .apply(String.format("[%s] WithVoid", name), WithKeys.of((Void) null));
  }

  static void writeToBigQuery(String name, PCollection<KV<Void, Long>> input, Write.Method writeMethod,
      String table) {
    PCollection<TableRow> tableRows = input
        .apply(String.format("[%s] ToTableRow", name), MapElements
            .via(new SimpleFunction<KV<Void, Long>, TableRow>() {
              @Override
              public TableRow apply(KV<Void, Long> input) {
                LOG.info("[%s] ToTableRow: {}", input);
                long value = Preconditions.checkNotNull(input.getValue());
                return new TableRow()
                    .set("k", String.format("%s-%d", name, value))
                    .set("v", value);
              }
            }));

    Write<TableRow> write = BigQueryIO.writeTableRows()
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .to(table)
        .withMethod(writeMethod);

    if (writeMethod != Write.Method.STREAMING_INSERTS) {
      // withTriggerFrequency required for unbound input to BigQueryIO with FILE_LOADS or
      // STORAGE_API, but it throws IllegalArgumentException with STREAMING_INSERTS.
      write = write.withTriggeringFrequency(Duration.standardSeconds(10));
    }

    tableRows.apply(String.format("[%s] Write", name), write);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    String table = Preconditions.checkNotNull(options.as(Options.class).getTable());
    Write.Method writeMethod = Preconditions.checkNotNull(
        options.as(Options.class).getWriteMethod());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<Void, Long>> source1 = createSource("Source1", pipeline.begin(), 1_000_000_000);
    PCollection<KV<Void, Long>> source2 = createSource("Source2", pipeline.begin(), 2_000_000_000);

    PCollection<KV<Void, Long>> flattened = PCollectionList.of(source1).and(source2)
        .apply(Flatten.pCollections());

    TupleTag<KV<Void, Long>> t1 = new TupleTag<KV<Void, Long>>() {};
    TupleTag<KV<Void, Long>> t2 = new TupleTag<KV<Void, Long>>() {};

    PCollectionTuple paths = flattened.apply("ToTwoOutputs",
        ParDo.of(new DoFn<KV<Void, Long>, KV<Void, Long>>() {
          @ProcessElement
          public void processElement(@Element KV<Void, Long> element,
              MultiOutputReceiver outputReceiver) {
            outputReceiver.get(t1).output(element);
            outputReceiver.get(t2).output(element);
          }
        }).withOutputTags(t1, TupleTagList.of(t2)));

    writeToBigQuery("Write1", paths.get(t1), writeMethod, table);
    writeToBigQuery("Write2", paths.get(t2), writeMethod, table);
    pipeline.run();
  }


  public interface Options extends PipelineOptions {

    @Description("BigQuery write method. Choose STORAGE_WRITE_API or STREAMING_INSERTS")
    Write.Method getWriteMethod();

    void setWriteMethod(Write.Method writeMethod);

    @Description("BigQuery table to write in the format of `project.dataset.table`")
    String getTable();

    void setTable(String table);
  }
}
