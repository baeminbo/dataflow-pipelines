package baeminbo;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.units.qual.Time;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class GroupPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(GroupPipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Read.from(CreateStreamingSource.of(1000, Duration.standardSeconds(1))))
        .apply(ParDo.of(new DoFn<Long, Message>() {
          @ProcessElement
          public void processElement(@Element Long index, @Timestamp Instant timestamp, BoundedWindow window, OutputReceiver<Message> out) {
            String s = "s" + index % 2;
            String t = "t" + index % 3;
            long l = index;
            Message message = new Message(s, t, l);
            LOG.info("Generate message:{}, with timestamp:{} and window:{}", message, timestamp, window);
            out.output(message);
          }
        }))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(Group.<Message>byFieldNames("s", "t")
            .aggregateField("l", Count.<Long>combineFn(), "count"))
        .apply(ParDo.of(new DoFn<Row, Void>() {
          @ProcessElement
          public void processElement(@Element Row row, @Timestamp Instant timestamp, BoundedWindow window) {
            LOG.info("row:{}, timestamp:{}, window: {}", row, timestamp, window);
          }
        }));
    pipeline.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class Message implements Serializable {
    @Nullable
    public String s;
    @Nullable
    public String t;
    @Nullable
    public Long l;

    public Message() {
    }

    public Message(@Nullable String s, @Nullable String t, @Nullable Long l) {
      this.s = s;
      this.t = t;
      this.l = l;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("s", s)
          .add("t", t)
          .add("l", l)
          .toString();
    }
  }
}
