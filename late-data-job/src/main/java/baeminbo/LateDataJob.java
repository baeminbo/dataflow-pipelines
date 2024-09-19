package baeminbo;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;

public class LateDataJob {
    private static final Logger LOG = LoggerFactory.getLogger(LateDataJob.class);

    private static final long START = 0;

    private static final long END = 200;

    private static final long LATE_START = 100;

    private static final Duration WINDOW_SIZE = Duration.standardSeconds(60);

    private static final Duration ALLOWED_LATENESS = Duration.standardSeconds(20);

    private static final String TIMER_NAME = "myTimer";

    private static final Duration TIMER_OFFSET = Duration.standardSeconds(30);

    private static final String STATE_NAME = "backlog";

    static {
        PipelineOptionsFactory.register(JobOptions.class);
    }

    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        boolean useGBK = options.as(JobOptions.class).getUseGBK();
        DoFnClass doFnClass = Preconditions.checkNotNull(options.as(JobOptions.class).getDoFnClass());

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Long> read = pipeline
                .apply(Read.from(new UnboundedLateElementSource(START, END, LATE_START)))
                // Trigger for each element for GBK. Trigger has no effect on StatefulDoFn.
                .apply(Window.<Long>into(FixedWindows.of(WINDOW_SIZE))
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(ALLOWED_LATENESS)
                        .discardingFiredPanes());


        PCollection<KV<String, Iterable<Long>>> shuffle;
        if (useGBK) {
            shuffle = read.apply(WithKeys.of("")).apply(GroupByKey.create());
        } else {
            shuffle = read.apply(MapElements.via(new SimpleFunction<Long, KV<String, Iterable<Long>>>() {
                @Override
                public KV<String, Iterable<Long>> apply(Long input) {
                    return KV.of("", Collections.singletonList(input));
                }
            }));
        }

        DoFn<KV<String, Iterable<Long>>, Void> doFnObj = doFnClass.getDoFnClass().getDeclaredConstructor().newInstance();
        shuffle.apply(ParDo.of(doFnObj));

        pipeline.run();
    }

    public interface JobOptions extends PipelineOptions {
        @Description("Add GroupByKey before DoFn.")
        boolean getUseGBK();

        void setUseGBK(boolean useGBK);

        @Description("DoFn class")
        DoFnClass getDoFnClass();

        void setDoFnClass(DoFnClass doFnClass);
    }

    public enum DoFnClass {
        PLAIN(PlainDoFn.class),
        TIMER(TimerDoFn.class),
        STATE(StateDoFn.class),
        STATE_TIMER(StateTimerDoFn.class);

        private final Class<? extends DoFn<KV<String, Iterable<Long>>, Void>> doFnClass;

        DoFnClass(Class<? extends DoFn<KV<String, Iterable<Long>>, Void>> doFnClass) {
            this.doFnClass = doFnClass;
        }

        public Class<? extends DoFn<KV<String, Iterable<Long>>, Void>> getDoFnClass() {
            return doFnClass;
        }
    }

    public static class PlainDoFn extends DoFn<KV<String, Iterable<Long>>, Void> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            LOG.info("receive window:{}, timestamp:{}, pane:{}, element:{}",
                    window, c.timestamp(), c.pane(), c.element());
        }

//        OnWindowExpiration cannot be used for DoFn without Timer and State
//        @OnWindowExpiration
//        public void onWindowExpiration(BoundedWindow window) {
//            LOG.info("window expired window:{}", window);
//        }
    }

    public static class TimerDoFn extends DoFn<KV<String, Iterable<Long>>, Void> {
        @TimerId(TIMER_NAME)
        private final TimerSpec myTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);


        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window, @TimerId(TIMER_NAME) Timer myTimer) {
            LOG.info("receive window:{}, timestamp:{}, pane:{}, element:{}",
                    window, c.timestamp(), c.pane(), c.element());

            myTimer.offset(TIMER_OFFSET).setRelative();
        }

        @OnTimer(TIMER_NAME)
        public void onTimer(OnTimerContext context) {
            LOG.info("timer triggered window:{}, fireTimestamp:{}, timestamp:{}",
                    context.window(), context.fireTimestamp(), context.timestamp());
        }

        @OnWindowExpiration
        public void onWindowExpiration(BoundedWindow window) {
            LOG.info("window expired window:{}", window);
        }
    }

    public static class StateDoFn extends DoFn<KV<String, Iterable<Long>>, Void> {
        @StateId(STATE_NAME)
        private final StateSpec<BagState<Long>> backlogStateSpec = StateSpecs.bag();

        @ProcessElement
        public void processElement(ProcessContext c,
                                   BoundedWindow window,
                                   @StateId(STATE_NAME) BagState<Long> state) {

            List<Long> currentBacklog = Lists.newArrayList(state.read());
            LOG.info("receive window:{}, timestamp:{}, pane:{}, element:{}, currentBacklog:{} ({})",
                    window, c.timestamp(), c.pane(), c.element(), currentBacklog.size(), currentBacklog);

            c.element().getValue().forEach(state::add);
        }

        @OnWindowExpiration
        public void onWindowExpiration(BoundedWindow window, @StateId(STATE_NAME) BagState<Long> state) {
            List<Long> currentBacklog = Lists.newArrayList(state.read());
            LOG.info("window expired window:{}, currentBacklog:{} ({})",
                    window, currentBacklog.size(), currentBacklog);
        }
    }

    public static class StateTimerDoFn extends DoFn<KV<String, Iterable<Long>>, Void> {
        @TimerId(TIMER_NAME)
        private final TimerSpec myTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
        @StateId(STATE_NAME)
        private final StateSpec<BagState<Long>> backlogStateSpec = StateSpecs.bag();

        @ProcessElement
        public void processElement(ProcessContext c,
                                   BoundedWindow window,
                                   @TimerId(TIMER_NAME) Timer myTimer,
                                   @StateId(STATE_NAME) BagState<Long> state) {

            List<Long> currentBacklog = Lists.newArrayList(state.read());
            LOG.info("receive window:{}, timestamp:{}, pane:{}, element:{}, currentBacklog:{} ({})",
                    window, c.timestamp(), c.pane(), c.element(), currentBacklog.size(), currentBacklog);

            c.element().getValue().forEach(state::add);
            myTimer.offset(TIMER_OFFSET).setRelative();
        }

        @OnTimer(TIMER_NAME)
        public void onTimer(OnTimerContext context, @StateId(STATE_NAME) BagState<Long> state) {
            List<Long> currentBacklog = Lists.newArrayList(state.read());
            LOG.info("timer triggered window:{}, fireTimestamp:{}, timestamp:{}, currentBacklog:{} ({})",
                    context.window(), context.fireTimestamp(), context.timestamp(), currentBacklog.size(),
                    currentBacklog);
        }

        @OnWindowExpiration
        public void onWindowExpiration(BoundedWindow window, @StateId(STATE_NAME) BagState<Long> state) {
            List<Long> currentBacklog = Lists.newArrayList(state.read());
            LOG.info("window expired window:{}, currentBacklog:{} ({})", window, currentBacklog.size(), currentBacklog);
        }
    }
}
