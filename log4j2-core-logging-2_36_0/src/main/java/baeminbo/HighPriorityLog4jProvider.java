package baeminbo;

import com.google.auto.service.AutoService;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.spi.Provider;

@AutoService(Provider.class)
public class HighPriorityLog4jProvider extends Provider {
  public HighPriorityLog4jProvider() {
    // Set a higher priority than SLF4JProvider (15). Log4jProvider has priority 10.
    super(100, "2.6.0", Log4jContextFactory.class);
    System.out.println("HighPriorityLog4JProvider Activated");
  }
}
