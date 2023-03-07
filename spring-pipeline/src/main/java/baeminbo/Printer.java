package baeminbo;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class Printer {

  private static final Logger LOG = LoggerFactory.getLogger(Printer.class);

  private final Level logLevel;
  private final String name; // randomly generated name

  public Printer(Level logLevel) {
    this.logLevel = logLevel;
    this.name = "Printer-" + new Random().nextInt(1000000);
    LOG.info("New printer created: {}", name);
  }

  public String getPrinterName() {
    return name;
  }

  public void print(String element) {
    switch (logLevel) {
      case TRACE:
        LOG.trace("[{}] element: {}", name, element);
        break;
      case DEBUG:
        LOG.debug("[{}] element: {}", name, element);
        break;
      case INFO:
        LOG.info("[{}] element: {}", name, element);
        break;
      case WARN:
        LOG.warn("[{}] element: {}", name, element);
        break;
      case ERROR:
        LOG.error("[{}] element: {}", name, element);
        break;
    }
  }
}
