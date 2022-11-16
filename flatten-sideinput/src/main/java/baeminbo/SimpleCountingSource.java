package baeminbo;

import baeminbo.SimpleCountingSource.Checkpoint;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SimpleCountingSource extends UnboundedSource<Long, Checkpoint> {

  private final long start;
  private final long end;
  private final Duration interval;

  public SimpleCountingSource(long start, long end, Duration interval) {
    this.start = start;
    this.end = end;
    this.interval = interval;
  }

  @Override
  public List<SimpleCountingSource> split(int desiredNumSplits,
      PipelineOptions options) {
    // No split
    return Collections.singletonList(this);
  }

  @Override
  public UnboundedReader<Long> createReader(PipelineOptions options,
      @Nullable Checkpoint checkpoint) {
    if (checkpoint != null) {
      return new Reader(checkpoint.getCheckpoint(), checkpoint.getLastAdvanceTime());
    } else {
      return new Reader(start, null);
    }
  }

  @Override
  public Coder<Checkpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(Checkpoint.class);
  }

  @Override
  public Coder<Long> getOutputCoder() {
    return VarLongCoder.of();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("start", start));
    builder.add(DisplayData.item("end", end));
    builder.add(DisplayData.item("interval", interval));
  }


  public static class Checkpoint implements Serializable, CheckpointMark {

    private final long checkpoint;
    private final @Nullable Instant lastAdvanceTime;

    public Checkpoint(long checkpoint, @Nullable Instant lastAdvanceTime) {
      this.checkpoint = checkpoint;
      this.lastAdvanceTime = lastAdvanceTime;
    }

    public long getCheckpoint() {
      return checkpoint;
    }

    public Instant getLastAdvanceTime() {
      return lastAdvanceTime;
    }

    @Override
    public void finalizeCheckpoint() {

    }
  }

  public class Reader extends UnboundedReader<Long> {

    private long current;
    private Instant lastAdvance;

    public Reader(long current, @Nullable Instant lastAdvance) {
      this.current = current;
      this.lastAdvance = lastAdvance;
    }

    @Override
    public boolean start() {
      // LOG.info("reader start current: {}", current);
      Preconditions.checkState(current <= end,
          "current '%s' is larger than end '%s'", current, end);
      Instant now = Instant.now();
      if (current < end && (lastAdvance == null || !lastAdvance.plus(interval).isAfter(now))) {
        // current < end && lastAdvance + interval <= now
        lastAdvance = now;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean advance() {
      // LOG.info("reader advance current: {}", current);
      Preconditions.checkState(current <= end,
          "current '%s' is larger than end '%s'", current, end);
      Instant now = Instant.now();
      if (current < end && (lastAdvance == null || !lastAdvance.plus(interval).isAfter(now))) {
        // current < end && lastAdvance + interval <= now
        ++current;
        lastAdvance = now;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return Instant.now();
    }

    @Override
    public void close() {
    }

    @Override
    public Instant getWatermark() {
      if (current < end) {
        return Instant.now();
      } else {
        // Set to the max timestamp. Dataflow streaming jobs will finish.
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
    }

    @Override
    public Checkpoint getCheckpointMark() {
      return new Checkpoint(current, lastAdvance);
    }

    @Override
    public UnboundedSource<Long, Checkpoint> getCurrentSource() {
      return SimpleCountingSource.this;
    }
  }
}

