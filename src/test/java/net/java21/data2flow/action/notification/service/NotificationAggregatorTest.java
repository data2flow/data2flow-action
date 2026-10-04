package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.notification.domain.DigestRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationAggregatorTest {

    @ParameterizedTest(name = "[RUL-03.06][BR-RUL-15][TC-RUL-083] 창을 닫을 때 기다린 {0}건 → {1}")
    @CsvSource({"0,NONE", "1,SINGLE", "2,SUMMARY", "7,SUMMARY", "12,SUMMARY"})
    void flush(int held, DigestRule.Flush expected) {
        assertThat(DigestRule.flush(held)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[OPS-06.04][BR-OPS-07][TC-OPS-067] 채널 기본 묶음: 창 안 {0}건 보냄, 기준 5, 한도 초과 {1} → 바로 보냄 {2}")
    @CsvSource({"0,false,true", "3,false,true", "4,false,false", "10,false,false", "0,true,false"})
    void channelDigest(int sent, boolean overRate, boolean now) {
        assertThat(DigestRule.sendNow(sent, 5, overRate)).isEqualTo(now);
    }

    @Test
    @DisplayName("[OPS-06.04][AT-OPS-12.3][TC-OPS-065] 1분 안 같은 알람 12건(채널 기본) → 앞 4건 바로, 나머지 8건은 창 끝에 요약 1건")
    void twelveAlarms() {
        int sentNow = 0;
        int held = 0;
        for (int i = 0; i < 12; i++) {
            if (DigestRule.sendNow(sentNow, 5, false)) {
                sentNow++;
            } else {
                held++;
            }
        }
        assertThat(sentNow).isEqualTo(4);
        assertThat(DigestRule.flush(held)).isEqualTo(DigestRule.Flush.SUMMARY);
        assertThat(held).isEqualTo(8);
    }
}
