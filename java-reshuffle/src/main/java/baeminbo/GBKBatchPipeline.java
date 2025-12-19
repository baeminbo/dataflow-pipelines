package baeminbo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class GBKBatchPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(GBKBatchPipeline.class);

  private static final int DEFAULT_ELEMENT_COUNT = 1_000_000;

  private static final int DEFAULT_ELEMENT_SIZE = 200;

  private static final int DEFAULT_LATENCY_MS = 3000;

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ReshuffleBatchPipelineOptions.class);

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    int elementCount = options.as(ReshuffleBatchPipelineOptions.class).getElementCount();
    int elementSize = options.as(ReshuffleBatchPipelineOptions.class).getElementSize();
    int latencyMs = options.as(ReshuffleBatchPipelineOptions.class).getLatencyMs();

    pipeline.apply(Create.of("")).apply(ParDo.of(new DoFn<String, byte[]>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        for (int i = 0; i < elementCount; ++i) {
          c.output(encode(i, elementSize));
        }
        LOG.info("Generated {} elements", elementCount);
      }
    })).apply("AddShardKey", ParDo.of(new DoFn<byte[], KV<Integer, byte[]>>() {
      int shard;

      @Setup
      public void setup() {
        shard = ThreadLocalRandom.current().nextInt();
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        ++shard;
        int hashOfShard = 0x1b873593 * Integer.rotateLeft(shard * 0xcc9e2d51, 15);
        c.output(KV.of(hashOfShard, c.element()));
      }
    })).apply(GroupByKey.create()).apply("IterateValues", ParDo.of(new DoFn<KV<Integer, Iterable<byte[]>>, byte[]>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        for (byte[] value : c.element().getValue()) {
          c.output(value);
        }
      }
    })).apply(ParDo.of(new DoFn<byte[], Long>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws InterruptedException, IOException {
        byte[] array = Objects.requireNonNull(c.element());
        long number = decode(array);
        LOG.info("[START] number:{}, size:{}, sleep:{}ms", number, array.length, latencyMs);
        Thread.sleep(latencyMs);
        LOG.info("[FINISH] number:{}", number);
      }
    }));

    pipeline.run();
  }

  /**
   * Creates a byte array of size `bytes`. The `number` is variably encoded into the initial bytes.
   */
  public static byte[] encode(long number, int bytes) throws IOException {
    byte[] array = new byte[bytes];
    VarLongCoder.of().encode(number, new OutputStream() {
      int pos = 0;

      @Override
      public void write(int b) {
        array[pos++] = (byte) b;
      }
    });
    return array;
  }

  /**
   * Decodes the variably-encoded number from the initial bytes.
   */
  public static long decode(byte[] array) throws IOException {
    ByteArrayInputStream input = new ByteArrayInputStream(array);
    return VarLongCoder.of().decode(input);
  }

  public interface ReshuffleBatchPipelineOptions extends PipelineOptions {

    @Description("The number of elements/")
    @Default.Integer(DEFAULT_ELEMENT_COUNT)
    int getElementCount();

    void setElementCount(int elementCount);

    @Description("Element byte size.")
    @Default.Integer(DEFAULT_ELEMENT_SIZE)
    int getElementSize();

    void setElementSize(int elementSize);

    @Description("Latency in milliseconds")
    @Default.Integer(DEFAULT_LATENCY_MS)
    int getLatencyMs();

    void setLatencyMs(int latencyMs);
  }
}
