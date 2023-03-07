package baeminbo.config;

import baeminbo.Printer;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

public class WarnPrinterConfig {

  @Bean
  Level printerLogLevel() {
    return Level.WARN;
  }

  @Bean
  Printer printer(@Autowired Level printerLogLevel) {
    return new Printer(printerLogLevel);
  }

}
