package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.action.common.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CircuitPolicyJsonTest {

    @Test
    @DisplayName("[ACT-07.03][TC-ACT-085] core 드라이버 서킷 {failureRate:50, windowSec:60, openSec:30}(minCalls 없음)을 읽으면 최소 호출 5건 기본값")
    void coreShapeWithoutMinCalls() {
        CircuitPolicy p = Json.read("{\"failureRate\":50,\"windowSec\":60,\"openSec\":30}", CircuitPolicy.class);
        assertThat(p).isEqualTo(new CircuitPolicy(50, 60, 30, 5));
    }

    @Test
    @DisplayName("[ACT-07.03] 이전 core가 저장한 비율(0.5)은 %(50)로 읽고, 빈 설정은 기본값")
    void legacyRatioAndEmpty() {
        assertThat(Json.read("{\"failureRate\":0.5,\"windowSec\":60,\"openSec\":30}", CircuitPolicy.class).failureRate()).isEqualTo(50.0);
        assertThat(Json.read("{}", CircuitPolicy.class)).isEqualTo(CircuitPolicy.DEFAULT);
    }

    @Test
    @DisplayName("[ACT-07.03] API-ACT-40 기기 제어 정보의 driver.circuit이 문서 모양이어도 ControlProfile을 읽는다")
    void controlProfileWithDocumentedCircuit() {
        ControlProfile profile = Json.read("""
                {"deviceId":6,"organizationId":1,"name":"에어컨","virtual":true,"status":"ACTIVE","capabilities":{},
                 "driver":{"driverId":3,"type":"VIRTUAL","config":{},"circuit":{"failureRate":0.5,"windowSec":60,"openSec":30}}}""",
                ControlProfile.class);
        assertThat(profile.driver().circuit()).isEqualTo(new CircuitPolicy(50, 60, 30, 5));
    }
}
