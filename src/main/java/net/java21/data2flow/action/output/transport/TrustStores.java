package net.java21.data2flow.action.output.transport;

import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

/** 출력 연결 비밀값 CA_CERT(PEM, 여러 개 가능)로 신뢰 저장소를 만든다. 없으면 JDK 기본 */
final class TrustStores {

    private TrustStores() {
    }

    static TrustManagerFactory fromPem(String pem) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<? extends Certificate> certs = cf.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            int i = 0;
            for (Certificate c : certs) {
                ks.setCertificateEntry("ca-" + i++, c);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            return tmf;
        } catch (Exception e) {
            throw new IllegalArgumentException("CA 인증서(PEM)를 읽을 수 없습니다", e);
        }
    }
}
