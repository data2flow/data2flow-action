package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.lorawan.ChirpStackHostGuard;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 공용 ChirpStack 다운링크 금지(CLAUDE.md §5, ⏸ ACT-03.03) */
class ChirpStackHostGuardTest {

    @ParameterizedTest
    @ValueSource(strings = {"https://s3.java21.net", "http://S3.Java21.Net:8080/api", "https://chirpstack.s3.java21.net",
            "https://iot-data.java21.net", "https://lns.example.org", "", "not a url", "http:///nohost"})
    @DisplayName("[ACT-03.03] 공용 ChirpStack·공용 브로커(하위 이름 포함)·설정 금지 주소·잘못된 주소는 거부")
    void denied(String url) {
        assertThatThrownBy(() -> ChirpStackHostGuard.requireAllowed(url, List.of("lns.example.org"))).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://localhost:8080", "https://chirpstack.internal.example"})
    @DisplayName("[ACT-03.03] 그 밖의 주소(시험용 목·전용 ChirpStack)는 허용")
    void allowed(String url) {
        assertThatCode(() -> ChirpStackHostGuard.requireAllowed(url, null)).doesNotThrowAnyException();
    }
}
