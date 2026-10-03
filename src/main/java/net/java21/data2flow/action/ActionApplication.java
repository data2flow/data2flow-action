package net.java21.data2flow.action;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** data2flow-action: 행동 실행: 제어 창구(Facade)·드라이버·명령 추적, 알림 발송(텔레그램, 채널 SPI), Sink·보내는 Webhook */
@SpringBootApplication
public class ActionApplication {

    public static void main(String[] args) {
        SpringApplication.run(ActionApplication.class, args);
    }
}
