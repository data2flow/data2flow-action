package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.action.common.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlarmInfoTest {

    @Test
    @DisplayName("[RUL-04.01][TC-RUL-084] 값이 없는 시스템 알람(게이트웨이 오프라인, API-RUL-41)도 읽는다 — 값은 null")
    void alarmWithoutValue() {
        AlarmInfo info = AlarmInfo.from(Json.MAPPER.readTree("""
                {"id":1,"alarmKey":"system:GATEWAY_OFFLINE:1","severity":"MAJOR","status":"ACTIVE","title":"게이트웨이 오프라인: GW1",
                 "occurrenceCount":1}"""));
        assertThat(info.value()).isNull();
        assertThat(info.title()).isEqualTo("게이트웨이 오프라인: GW1");
    }

    @Test
    @DisplayName("[RUL-03.01] 마지막 값이 있으면 그 값, 없으면 발생 값")
    void valuePreference() {
        assertThat(AlarmInfo.from(Json.MAPPER.readTree("{\"id\":2,\"lastValue\":29.5,\"triggerValue\":28}")).value()).isEqualTo(29.5);
        assertThat(AlarmInfo.from(Json.MAPPER.readTree("{\"id\":3,\"triggerValue\":28}")).value()).isEqualTo(28.0);
    }
}
