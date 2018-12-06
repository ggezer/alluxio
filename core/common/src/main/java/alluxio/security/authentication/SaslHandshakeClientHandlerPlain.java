package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

public class SaslHandshakeClientHandlerPlain implements SaslHandshakeClientHandler {

  private SaslClient mSaslClient;

  public SaslHandshakeClientHandlerPlain(SaslClient saslClient) {
    mSaslClient = saslClient;
  }

  @Override
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException {
    switch (message.getMessageType()) {
      case CHALLANGE:
        byte[] saslResponse = mSaslClient.evaluateChallenge(message.getMessage().toByteArray());
        SaslMessage.Builder response =
            SaslMessage.newBuilder().setMessageType(SaslMessageType.CHALLANGE);
        if (saslResponse != null) {
          response.setMessage(ByteString.copyFrom(saslResponse));
        }
        return response.build();
      case SUCCESS:
        if (message.hasMessage()) {
          mSaslClient.evaluateChallenge(message.getMessage().toByteArray());
        }
        Preconditions.checkArgument(mSaslClient.isComplete());
        return null;
      default:
        throw new SaslException("Don't know how to process");
    }
  }

  @Override
  public SaslMessage getInitialMessage(String clientId) throws SaslException {
    byte[] initiateSaslResponse = null;
    if (mSaslClient.hasInitialResponse()) {
      initiateSaslResponse = mSaslClient.evaluateChallenge(new byte[0]);
    }
    SaslMessage.Builder initialResponse =
        SaslMessage.newBuilder().setMessageType(SaslMessageType.CHALLANGE)
            .setAuthenticationName(AuthType.SIMPLE.getAuthName());
    if (initiateSaslResponse != null) {
      initialResponse.setMessage(ByteString.copyFrom(initiateSaslResponse));
    }
    initialResponse.setClientId(clientId);
    return initialResponse.build();
  }
}
