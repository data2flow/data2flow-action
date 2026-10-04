package net.java21.data2flow.action.output.transport;

import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import net.java21.data2flow.contracts.output.OutputFailureKind;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.TimeoutException;

/** 예외 → 출력 실패 종류(API-DSC-32 {@code failureKind}) */
final class FailureKinds {

    private FailureKinds() {
    }

    static OutputFailureKind of(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof Mqtt5ConnAckException ack) {
                Mqtt5ConnAckReasonCode code = ack.getMqttMessage().getReasonCode();
                return code == Mqtt5ConnAckReasonCode.NOT_AUTHORIZED || code == Mqtt5ConnAckReasonCode.BAD_USER_NAME_OR_PASSWORD
                        || code == Mqtt5ConnAckReasonCode.BAD_AUTHENTICATION_METHOD ? OutputFailureKind.AUTH : OutputFailureKind.REFUSED;
            }
            if (t instanceof UnknownHostException || t instanceof UnresolvedAddressException) {
                return OutputFailureKind.DNS;
            }
            if (t instanceof SSLException) {
                return OutputFailureKind.TLS;
            }
            if (t instanceof TimeoutException || t instanceof HttpTimeoutException) {
                return OutputFailureKind.TIMEOUT;
            }
        }
        // JDK HttpClient는 이름 풀기 실패도 ConnectException(원인 UnresolvedAddressException)으로 감싸므로 연결 거부는 원인을 다 본 뒤에 판정한다
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof ConnectException) {
                return OutputFailureKind.REFUSED;
            }
        }
        String msg = error == null ? "" : String.valueOf(error.getMessage()).toLowerCase(java.util.Locale.ROOT);
        if (msg.contains("timed out") || msg.contains("timeout")) {
            return OutputFailureKind.TIMEOUT;
        }
        if (msg.contains("ssl") || msg.contains("certificate") || msg.contains("handshake")) {
            return OutputFailureKind.TLS;
        }
        return OutputFailureKind.REFUSED;
    }

    static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
        return msg.length() > 500 ? msg.substring(0, 500) : msg;
    }
}
