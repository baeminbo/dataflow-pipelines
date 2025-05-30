import com.google.api.gax.httpjson.HttpJsonCallContext;
import com.google.api.gax.httpjson.HttpJsonCallOptions;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.spanner.v1.stub.SpannerStub;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;
import com.google.protobuf.ListValue;
import com.google.protobuf.TypeRegistry;
import com.google.protobuf.Value;
import com.google.rpc.ErrorDetailsProto;
import com.google.spanner.v1.BatchWriteRequest;
import com.google.spanner.v1.BatchWriteResponse;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.Mutation;
import com.google.spanner.v1.Session;

import java.io.IOException;

public class SpannerHttpJsonStubMainFixed {
  public static void main(String[] args) throws IOException {
    SpannerStubSettings stubSettings = SpannerStubSettings.newHttpJsonBuilder().build();
    try (SpannerStub stub = stubSettings.createStub()) {
      Session session = stub.createSessionCallable()
          .call(CreateSessionRequest.newBuilder()
              .setDatabase("projects/<REDACTED>/instances/spanner-1/databases/db-1")
              .build());
      BatchWriteRequest request = BatchWriteRequest.newBuilder()
          .setSession(session.getName())
          .addMutationGroups(BatchWriteRequest.MutationGroup.newBuilder()
              .addMutations(Mutation.newBuilder()
                  .setInsert(Mutation.Write.newBuilder()
                      // SCHEMA: CREATE TABLE table1 (k STRING(100), v INT64) PRIMARY KEY (k)
                      .setTable("table1")
                      .addColumns("k")
                      .addColumns("v")
                      // The key "k1" already exists in the table. Therefore, this request will result in an error
                      // response.
                      .addValues(ListValue.newBuilder()
                          .addValues(Value.newBuilder().setStringValue("k1"))
                          .addValues(Value.newBuilder().setStringValue("1"))))))
          .build();

      ServerStreamingCallable<BatchWriteRequest, BatchWriteResponse> callable = stub.batchWriteCallable();

      TypeRegistry registry = TypeRegistry.newBuilder().add(ErrorDetailsProto.getDescriptor().getMessageTypes()).build();
      HttpJsonCallContext callContext = HttpJsonCallContext.createDefault()
          .withCallOptions(HttpJsonCallOptions.newBuilder().setTypeRegistry(registry).build());
      ServerStream<BatchWriteResponse> responseStream = callable.call(request, callContext);
      for (BatchWriteResponse response : responseStream) {
        System.out.println("response = " + response);
      }
    }
  }
}
