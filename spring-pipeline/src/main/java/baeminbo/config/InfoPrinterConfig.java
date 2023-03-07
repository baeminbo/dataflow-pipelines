package baeminbo.config;

import baeminbo.Printer;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

public class InfoPrinterConfig {
  @Bean
  Level printerLogLevel() {
    return Level.INFO;
  }

  @Bean
  Printer printer(@Autowired Level printerLogLevel) {
    return new Printer(printerLogLevel);
  }
}
