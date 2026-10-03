package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.command.SourceType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.DisplayName;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** 샌드박스(BR-ACT-23, SIM-07.03). TC-ACT-024·029 */
class SandboxGuardTest {

    @ParameterizedTest(name = "출처 {0}, 공간 {1}, 가상 {2} → 막힘 {3}")
    @CsvSource({
            "FLOW,31,false,true", "FLOW,31,true,false", "FLOW,40,false,false", "RULE,31,false,true", "SCHEDULE,31,false,true",
            "AI,31,false,true", "USER,31,false,false", "BULK,31,false,false", "SYSTEM,31,false,false", "FLOW,,false,false"})
    @DisplayName("[ACT-02.01][AT-ACT-15.1][TC-ACT-029] 샌드박스 출처 + 실제 기기만 거부, 사용자 수동 제어는 미적용")
    void rules(SourceType type, Long sourceSpace, boolean virtual, boolean forbidden) {
        assertThat(SandboxGuard.forbidden(Set.of(31L), type, sourceSpace, virtual)).isEqualTo(forbidden);
    }
}
