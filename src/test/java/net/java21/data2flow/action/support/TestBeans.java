package net.java21.data2flow.action.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** 시험 빈: 운영 시계 대신 MutableClock */
@TestConfiguration(proxyBeanMethods = false)
public class TestBeans {

    @Bean
    @Primary
    MutableClock mutableClock() {
        return new MutableClock(MutableClock.T0);
    }

    /** 알림 채널 SPI 시험용 가짜 채널(키 FAKE, 버튼 없음, TC-OPS-141). 공통 계층이 채널 종류로 분기하지 않음을 보인다 */
    @Bean
    net.java21.data2flow.contracts.test.notification.FakeNotificationChannel fakeNotificationChannel() {
        return new net.java21.data2flow.contracts.test.notification.FakeNotificationChannel();
    }
}
