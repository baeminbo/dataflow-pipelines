package baeminbo;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class CreateStreamingSource {

  public static StreamingSource of(long stop, Duration sleepDuration) {
    return new StreamingSource(stop, sleepDuration);
  }

  static class Checkpoint implements CheckpointMark, Serializable {

    private static final long serialVersionUID = 3608738320331385333L;

    public long current;

    @Override
    public void finalizeCheckpoint() throws IOException {

    }

    public long getCurrent() {
      return current;
    }

    public void setCurrent(long current) {
      this.current = current;
    }
  }

  /**
   * Unsplittable streaming source generating longs with current timestamp as watermark.
   */
  public static class StreamingSource extends UnboundedSource<Long, Checkpoint> {

    private static final long serialVersionUID = -8588693923874163506L;
    private final long stop;
    private final Duration sleep;

    StreamingSource(long stop, Duration sleep) {
      if (stop < 0) {
        throw new IllegalArgumentException("`stop` must not be negative, but given " + stop);
      }
      this.stop = stop;
      this.sleep = sleep;
    }

    public long getStop() {
      return stop;
    }

    public Duration getSleep() {
      return sleep;
    }

    @Override
    public List<? extends UnboundedSource<Long, Checkpoint>> split(int desiredNumSplits,
        PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Long> createReader(PipelineOptions options,
        @Nullable Checkpoint checkpoint) {
      return new UnboundedReader<Long>() {
        // continue from the checkpoint or start with 0.
        private long current = checkpoint != null ? checkpoint.getCurrent() : 0;

        @Override
        public boolean start() {
          return current < stop;
        }

        @Override
        public boolean advance() {
          if (current < stop) {
            ++current;

            try {
              Thread.sleep(sleep.getMillis());
            } catch (InterruptedException ignored) {
            }
            return true;
          } else {
            return false;
          }
        }

        @Override
        public Instant getWatermark() {
          return Instant.now();
        }

        @Override
        public CheckpointMark getCheckpointMark() {
          Checkpoint newCheckpoint = new Checkpoint();
          newCheckpoint.setCurrent(current);
          return newCheckpoint;
        }

        @Override
        public UnboundedSource<Long, ?> getCurrentSource() {
          return StreamingSource.this;
        }

        @Override
        public Long getCurrent() throws NoSuchElementException {
          return current;
        }

        @Override
        public Instant getCurrentTimestamp() {
          return Instant.now();
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public Coder<Checkpoint> getCheckpointMarkCoder() {
      return SerializableCoder.of(Checkpoint.class);
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }
  }
}
