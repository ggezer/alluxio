package alluxio.security.authentication;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.UUID;

/**
 * Client side interceptor that is used to augment outgoing metadata with the unique id for the
 * channel that the RPC is being called on.
 */
public class ChannelIdInjector implements ClientInterceptor {

  /** Metadata key for the channel Id */
  public static Metadata.Key<UUID> sClientIdKey =
      Metadata.Key.of("channel-id", new Metadata.AsciiMarshaller<UUID>() {
        @Override
        public String toAsciiString(UUID value) {
          return value.toString();
        }

        @Override
        public UUID parseAsciiString(String serialized) {
          return UUID.fromString(serialized);
        }
      });

  private UUID mChannelId;

  /**
   * Creates the injector that augments the outgoing metadata with given Id.
   * 
   * @param channelId channel id
   */
  public ChannelIdInjector(UUID channelId) {
    mChannelId = channelId;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        // Put channel Id to headers.
        headers.put(sClientIdKey, mChannelId);
        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
            responseListener) {
          @Override
          public void onHeaders(Metadata headers) {
            super.onHeaders(headers);
          }
        }, headers);
      }
    };
  }
}