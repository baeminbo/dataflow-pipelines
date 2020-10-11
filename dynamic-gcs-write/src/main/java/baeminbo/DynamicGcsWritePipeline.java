package baeminbo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;

public class DynamicGcsWritePipeline {

  private static final long INPUT_ELEMENT_COUNT = 1_000_000_000;
  private static final int SHARD_COUNT = 100;
  private static final int DESTINATION_COUNT = 10_000;

  public interface Options extends PipelineOptions {

    @Description("The number of input elements. Default is " + INPUT_ELEMENT_COUNT)
    @Default.Long(INPUT_ELEMENT_COUNT)
    ValueProvider<Long> getInputElementCount();

    void setInputElementCount(ValueProvider<Long> inputElementCount);

    @Description("The number of shards, which may be the number of final GCS shards. Default is "
        + SHARD_COUNT)
    @Default.Integer(SHARD_COUNT)
    ValueProvider<Integer> getShardCount();

    void setShardCount(ValueProvider<Integer> shardCount);

    @Description("The number of destination. Default is " + DESTINATION_COUNT)
    @Default.Integer(DESTINATION_COUNT)
    ValueProvider<Integer> getDestinationCount();

    void setDestinationCount(ValueProvider<Integer> destinationCount);

    @Description("[Required] Output location. For example, \"gs://byop-exercise/output/\"")
    ValueProvider<String> getOutputLocation();

    void setOutputLocation(ValueProvider<String> outputLocation);
  }

  public static class Input extends BoundedSource<Long> {

    private final ValueProvider<Long> elementCount;

    public Input(ValueProvider<Long> elementCount) {
      this.elementCount = elementCount;
    }

    @Override
    public List<? extends BoundedSource<Long>> split(long desiredBundleSizeBytes,
        PipelineOptions options) {
      // No split
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      if (elementCount.isAccessible()) {
        return CountingSource.upTo(elementCount.get()).getEstimatedSizeBytes(options);
      } else {
        return 0L; // unknown
      }
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public BoundedReader<Long> createReader(PipelineOptions options) throws IOException {
      if (!elementCount.isAccessible()) {
        throw new IllegalStateException("elementCount is not accessible");
      }
      return CountingSource.upTo(elementCount.get()).createReader(options);
    }
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollectionView<Integer> destinationCountView = pipeline
        .apply("DestinationCount",
            Create.ofProvider(options.getDestinationCount(), VarIntCoder.of()))
        .apply("DestinationCountView", View.asSingleton());

    pipeline
        .apply("Read", Read.from(new Input(options.getInputElementCount())))
        .apply("Write", FileIO.<Long, Long>writeDynamic()
            .by(Contextful.fn(
                new Fn<Long, Long>() {
                  private MessageDigest hasher;

                  private long hash(long element) throws NoSuchAlgorithmException {
                    if (hasher == null) {
                      hasher = MessageDigest.getInstance("SHA-256");
                    }

                    byte[] inputBytes = ByteBuffer.allocate(Long.BYTES).putLong(element).array();
                    byte[] hashBytes = hasher.digest(inputBytes);
                    return ByteBuffer.wrap(hashBytes).getLong(); // may take first 8 bytes as output
                  }

                  @Override
                  public Long apply(Long element, Context c) throws Exception {
                    Integer destinationCount = c.sideInput(destinationCountView);
                    long hash = hash(element);
                    return Math.floorMod(hash, (long)destinationCount);
                  }
                }, Requirements.requiresSideInputs(destinationCountView)))
            .withDestinationCoder(VarLongCoder.of())
            .withNumShards(options.getShardCount())
            .withNaming(Contextful.fn(
                (Fn<Long, FileNaming>) (destination, context) -> {
                  Integer destinationCount = context.sideInput(destinationCountView);
                  return Write
                      .defaultNaming(String.format("%08d-of-%08d", destination, destinationCount),
                          "");
                }, Requirements.requiresSideInputs(destinationCountView)))
            .via(Contextful.fn((SerializableFunction<Long, String>) Object::toString),
                TextIO.sink())
            .to(options.getOutputLocation()));

    pipeline.run();
  }
}
