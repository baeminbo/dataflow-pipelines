Run `run.sh`. You may see Dataflow job creation failed with the following error. What happened?

```
INFO: Adding FileIO.Match/Via MatchAll/Continuously match filepatterns/ParDo(WatchGrowth)/ParMultiDo(WatchGrowth)/Explode windows as step s5
Jan 05, 2021 8:46:41 PM org.apache.beam.runners.dataflow.DataflowPipelineTranslator$Translator addStep
INFO: Adding FileIO.Match/Via MatchAll/Continuously match filepatterns/ParDo(WatchGrowth)/ParMultiDo(WatchGrowth)/Assign unique key/AddKeys/Map as step s6
Exception in thread "main" java.lang.IllegalArgumentException: Expected DoFn to be FunctionSpec with URN beam:dofn:javasdk:0.1, but URN was 
	at org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument(Preconditions.java:477)
	at org.apache.beam.runners.core.construction.ParDoTranslation.doFnWithExecutionInformationFromProto(ParDoTranslation.java:697)
	at org.apache.beam.runners.core.construction.ParDoTranslation.getSchemaInformation(ParDoTranslation.java:389)
	at org.apache.beam.runners.core.construction.ParDoTranslation.getSchemaInformation(ParDoTranslation.java:374)
	at org.apache.beam.runners.dataflow.DataflowPipelineTranslator$11.translateTyped(DataflowPipelineTranslator.java:1158)
	at org.apache.beam.runners.dataflow.DataflowPipelineTranslator$11.translate(DataflowPipelineTranslator.java:1148)
	at org.apache.beam.runners.dataflow.DataflowPipelineTranslator$11.translate(DataflowPipelineTranslator.java:1144)
	at org.apache.beam.runners.dataflow.DataflowPipelineTranslator$Translator.visitPrimitiveTransform(DataflowPipelineTranslator.java:484)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:664)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:656)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:656)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:656)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:656)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:656)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.visit(TransformHierarchy.java:656)
	at org.apache.beam.sdk.runners.TransformHierarchy$Node.access$600(TransformHierarchy.java:317)
	at org.apache.beam.sdk.runners.TransformHierarchy.visit(TransformHierarchy.java:251)
	at org.apache.beam.sdk.Pipeline.traverseTopologically(Pipeline.java:463)
	at org.apache.beam.runners.dataflow.DataflowPipelineTranslator$Translator.translate(DataflowPipelineTranslator.java:423)
	at org.apache.beam.runners.dataflow.DataflowPipelineTranslator.translate(DataflowPipelineTranslator.java:182)
	at org.apache.beam.runners.dataflow.DataflowRunner.run(DataflowRunner.java:910)
	at org.apache.beam.runners.dataflow.DataflowRunner.run(DataflowRunner.java:195)
	at org.apache.beam.sdk.Pipeline.run(Pipeline.java:317)
	at org.apache.beam.sdk.Pipeline.run(Pipeline.java:303)
	at byop.UrnNotFoundPipeline.main(UrnNotFoundPipeline.java:64)
```
