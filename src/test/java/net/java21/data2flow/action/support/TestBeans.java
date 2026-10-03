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
}
