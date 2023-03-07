package baeminbo;

import com.google.common.base.Preconditions;
import org.apache.beam.sdk.transforms.DoFn;
import org.springframework.beans.factory.annotation.Autowired;

public class ProcessDoFn<T> extends DoFn<T, Void> {
  @Autowired
  Printer printer;

  @Setup
  public void setUp() {
    // SetUp called only one time after a DoFn instance is created and before a bundle starts.
    // This is best position to auto-wire the injected objects.
    PipelineApplicationContext.getInstance().getAutowireCapableBeanFactory().autowireBean(this);
  }

  @ProcessElement
  public void processElement(@Element T element) throws InterruptedException {
    Preconditions.checkNotNull(printer);
    // Sleep for a long process time so that dynamic rebalancing splits the work item.
    Thread.sleep(10000);
    printer.print(element.toString());
  }
}
