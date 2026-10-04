package net.java21.data2flow.action;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import net.java21.data2flow.contracts.test.arch.Data2flowArchRules;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * 공통 ArchUnit 규칙(testing/backend.md §6)과 제어 창구 규칙(ACT-02.01 "드라이버를 직접 부르는 경로는 없다"), 공용 브로커 안전(CLAUDE.md §5).
 */
@AnalyzeClasses(packages = "net.java21.data2flow.action", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule organizationScoped = Data2flowArchRules.REPOSITORY_QUERIES_ARE_ORGANIZATION_SCOPED;
    @ArchTest
    static final ArchRule noUnscopedCrud = Data2flowArchRules.UNSCOPED_CRUD_LOOKUPS_ARE_NOT_CALLED;
    @ArchTest
    static final ArchRule noSleep = Data2flowArchRules.NO_THREAD_SLEEP;
    @ArchTest
    static final ArchRule noSystemClock = Data2flowArchRules.NO_SYSTEM_CLOCK;

    /** [ACT-02.01][BR-ACT-01] 드라이버 실행(execute)은 창구의 드라이버 호출 단계(CommandDispatcher)만 부른다 */
    @ArchTest
    static final ArchRule onlyFacadeCallsDrivers = noClasses()
            .that().doNotHaveFullyQualifiedName("net.java21.data2flow.action.actuation.service.CommandDispatcher")
            .should().callMethod(net.java21.data2flow.action.actuation.driver.DeviceDriver.class, "execute",
                    net.java21.data2flow.action.actuation.driver.DriverCommand.class)
            .because("모든 명령은 제어 창구를 거친다(ACT-02.01, ADR-009)");

    /** [ACT-03.02][DSC-04.01] MQTT 클라이언트는 MQTT 드라이버와 출력 연결 발송 패키지에만(공용 브로커 금지 장치가 있는 곳) */
    @ArchTest
    static final ArchRule mqttOnlyInDriver = noClasses()
            .that().resideOutsideOfPackages("..actuation.driver.mqtt..", "..output.transport..")
            .should().dependOnClassesThat().resideInAnyPackage("com.hivemq..", "org.eclipse.paho..")
            .because("공용 브로커 iot-data.java21.net 발행 금지(CLAUDE.md §5): 금지 장치(MqttBrokerGuard·output HostGuard)를 거치는 곳만 MQTT를 쓴다");

    /** [DSC-04.01] 출력 연결은 제어·알림 패키지와 분리한다(M5, milestones.md): output은 actuation·notification·sink에 기대지 않는다 */
    @ArchTest
    static final ArchRule outputIsSeparate = noClasses()
            .that().resideInAPackage("..action.output..")
            .should().dependOnClassesThat().resideInAnyPackage("..action.actuation..", "..action.notification..", "..action.sink..")
            .because("출력 연결은 M4가 고치는 actuation·notification과 별도 패키지다");
}
