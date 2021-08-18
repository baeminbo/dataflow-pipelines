package baeminbo;

import com.google.auto.service.AutoService;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableWritePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableWritePipeline.class);

  // Specified inside Dataflow worker. Null during pipeline creation.
  private static PipelineOptions runtimePipelineOptions = null;

  public static void main(String[] args) {
    BigtableWritePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        .as(BigtableWritePipelineOptions.class);

    // Call getXXX for options used in DoFn only to avoid https://issues.apache.org/jira/browse/BEAM-7983
    options.getBigtableAppProfileId();
    options.getBigtableColumnFamily();

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(KV.of("first", "hello"), KV.of("second", "world")))
        .apply(ParDo.of(new DoFn<KV<String, String>, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            // Put a cell to row key: <key of input>, column family: <value of options>, column:
            // "vale", value: <value of input>
            String key = context.element().getKey();
            String value = context.element().getValue();
            String columnFamily = context.getPipelineOptions()
                .as(BigtableWritePipelineOptions.class).getBigtableColumnFamily().get();

            ByteString row = ByteString.copyFromUtf8(key);
            Mutation mutation = Mutation.newBuilder()
                .setSetCell(Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily)
                    .setColumnQualifier(ByteString.copyFromUtf8("value"))
                    .setValue(ByteString.copyFromUtf8(value))
                    .build())
                .build();
            context.output(KV.of(row, Collections.singletonList(mutation)));
          }
        }))
        .apply(BigtableIO.write()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstance())
            .withTableId(options.getBigtableTable())
            .withBigtableOptionsConfigurator(builder -> {
              if (runtimePipelineOptions != null) {
                @Nullable String appProfileId =
                    runtimePipelineOptions.as(BigtableWritePipelineOptions.class)
                        .getBigtableAppProfileId().get();
                if (appProfileId != null && !appProfileId.isEmpty()) {
                  LOG.info("Override AppProfile ID with '{}'", appProfileId);
                  builder.setAppProfileId(appProfileId);
                } else {
                  LOG.info("Use default AppProfile ID. No AppProfile ID was set in options");
                }
              } else {
                LOG.warn("Use default AppProfile ID because runtime option is not set properly");
              }
              return builder;
            }));

    pipeline.run();
  }

  public interface BigtableWritePipelineOptions extends PipelineOptions {

    @Description("Bigtable project")
    ValueProvider<String> getBigtableProject();

    void setBigtableProject(ValueProvider<String> bigtableProject);

    @Description("Bigtable instance id")
    ValueProvider<String> getBigtableInstance();

    void setBigtableInstance(ValueProvider<String> bigtableInstance);

    @Description("Bigtable table")
    ValueProvider<String> getBigtableTable();

    void setBigtableTable(ValueProvider<String> bigtableTable);

    @Description("Bigtable column family")
    ValueProvider<String> getBigtableColumnFamily();

    void setBigtableColumnFamily(ValueProvider<String> bigtableColumnFamily);

    @Description("Bigtable AppProfileId")
    ValueProvider<String> getBigtableAppProfileId();

    void setBigtableAppProfileId(ValueProvider<String> bigtableAppProfileId);
  }

  /**
   * Add dependency. For example, for Maven ```
   * <dependency>
   * <groupId>com.google.auto.service</groupId>
   * <artifactId>auto-service</artifactId>
   * <version>1.0</version>
   * </dependency>
   * ```
   */
  @AutoService(JvmInitializer.class)
  public static class PipelineOptionSetter implements JvmInitializer {

    @Override
    public void beforeProcessing(PipelineOptions options) {
      runtimePipelineOptions = options;
      LOG.info("Set runtimePipelineOptions: {}", options);
    }
  }
}
