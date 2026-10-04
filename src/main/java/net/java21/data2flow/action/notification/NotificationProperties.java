package net.java21.data2flow.action.notification;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * 알림 공통 계층 설정({@code data2flow.action.notification.*}). 비밀값(봇 토큰)은 여기 두지 않는다: 채널 정의와 함께 core가 준다(API-OPS-35).
 *
 * @param retryBackoffs         재시도 간격. 길이가 최대 재시도 횟수(BR-RUL-17·BR-OPS-06·OPS-06.03: 30초·2분·10분·30분·1시간, 5회)
 * @param digestThreshold       채널 기본 묶음: 창 안 같은 묶음 키의 알림이 이 수 이상이면 요약(BR-OPS-07, 5). 앞 (n-1)건은 바로 보낸다
 * @param defaultDigestWindow   채널 설정이 없을 때 묶음 창(60초)
 * @param defaultRenotify       정책이 없을 때 재알림 간격(30분, BR-RUL-13)
 * @param virtualMuted          가상 기기 알람 알림 끄기(SIM-07.04). false면 제목에 "[가상] " 접두어
 * @param webBaseUrl            바로가기 링크 기준 주소(https://data2flow.java21.net)
 * @param defaultLocale         사용자 언어가 없을 때(ko)
 * @param cacheTtl              core 정의 캐시 최대 수명(설정 변경 메시지를 놓쳐도 이 시간 안에 다시 읽음)
 * @param sendLease             보내는 중 표시(다른 파드가 겹쳐 보내지 않게 미루는 시간)
 * @param batch                 주기 작업 한 번에 처리할 행 수
 * @param telegram              텔레그램 채널 어댑터
 */
@ConfigurationProperties(prefix = "data2flow.action.notification")
public record NotificationProperties(List<Duration> retryBackoffs, Integer digestThreshold, Duration defaultDigestWindow,
                                     Duration defaultRenotify, boolean virtualMuted, String webBaseUrl, String defaultLocale,
                                     Duration cacheTtl, Duration sendLease, Integer batch, Telegram telegram) {

    public static final List<Duration> SPEC_BACKOFFS = List.of(Duration.ofSeconds(30), Duration.ofMinutes(2), Duration.ofMinutes(10),
            Duration.ofMinutes(30), Duration.ofHours(1));

    public NotificationProperties {
        retryBackoffs = retryBackoffs == null || retryBackoffs.isEmpty() ? SPEC_BACKOFFS : List.copyOf(retryBackoffs);
        digestThreshold = digestThreshold == null || digestThreshold < 2 ? 5 : digestThreshold;
        defaultDigestWindow = defaultDigestWindow == null ? Duration.ofSeconds(60) : defaultDigestWindow;
        defaultRenotify = defaultRenotify == null ? Duration.ofMinutes(30) : defaultRenotify;
        webBaseUrl = webBaseUrl == null || webBaseUrl.isBlank() ? "https://data2flow.java21.net" : webBaseUrl;
        defaultLocale = defaultLocale == null || defaultLocale.isBlank() ? "ko" : defaultLocale;
        cacheTtl = cacheTtl == null ? Duration.ofSeconds(60) : cacheTtl;
        sendLease = sendLease == null ? Duration.ofMinutes(2) : sendLease;
        batch = batch == null || batch < 1 ? 100 : batch;
        telegram = telegram == null ? new Telegram(false, null, null) : telegram;
    }

    public static NotificationProperties defaults() {
        return new NotificationProperties(null, null, null, null, false, null, null, null, null, null, null);
    }

    /**
     * 텔레그램 Bot API 어댑터(OPS-06.01). 실제 봇 토큰이 아직 없어서 기본은 꺼짐: 꺼져 있으면 {@code available()=false}이고 발송은
     * 영구 실패(CHANNEL_UNAVAILABLE)로 끝난다. 시험은 MockWebServer 주소를 넣는다.
     *
     * @param enabled    켜기(기본 false)
     * @param apiBaseUrl Bot API 주소(https://api.telegram.org)
     * @param webhookUrl setWebhook으로 등록할 콜백 주소(https://data2flow.java21.net/hooks/messenger/telegram, ADR-029)
     */
    public record Telegram(boolean enabled, String apiBaseUrl, String webhookUrl) {
        public Telegram {
            apiBaseUrl = apiBaseUrl == null || apiBaseUrl.isBlank() ? "https://api.telegram.org" : apiBaseUrl;
            webhookUrl = webhookUrl == null || webhookUrl.isBlank() ? "https://data2flow.java21.net/hooks/messenger/telegram" : webhookUrl;
        }
    }
}
