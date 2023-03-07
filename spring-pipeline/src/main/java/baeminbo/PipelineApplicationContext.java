package baeminbo;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.Collections;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.Validation.Required;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Manages a singleton instance of {@link ApplicationContext}.
 * <p/>
 * Dataflow initializes the application context with the value of {@link
 * AutowireOptions#getAutowireConfiguration()} through {@link ContextInitializer} at SDK worker
 * startup.
 * <p/>
 * The {@link AutowireOptions} requires a configuration class for autowiring.
 * <p/>
 * Use {@link #initialize(ApplicationContext)} for testing.
 */
public class PipelineApplicationContext {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineApplicationContext.class);

  private static ApplicationContext context;

  public static synchronized ApplicationContext getInstance() {
    Preconditions.checkNotNull(context);
    return context;
  }

  public static synchronized void initialize(ApplicationContext context) {
    PipelineApplicationContext.context = context;
  }


  public interface AutowireOptions extends PipelineOptions {
    @Description("Spring configuration class name for autowiring.")
    @Required
    Class<?> getAutowireConfiguration();

    void setAutowireConfiguration(Class<?> autowireConfiguration);
  }

  @AutoService(JvmInitializer.class)
  public static class ContextInitializer implements JvmInitializer {
    @Override
    public void beforeProcessing(PipelineOptions options) {
      LOG.info("Initializing application context.");
      Class<?> configurationClass = options.as(AutowireOptions.class).getAutowireConfiguration();
      ApplicationContext context = new AnnotationConfigApplicationContext(configurationClass);
      initialize(context);
    }
  }

  @AutoService(PipelineOptionsRegistrar.class)
  public static class AutowireOptionsRegistrar implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      LOG.info("Register AutowireOptions.");
      return Collections.singletonList(AutowireOptions.class);
    }
  }
}
