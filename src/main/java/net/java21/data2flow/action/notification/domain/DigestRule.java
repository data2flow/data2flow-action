package net.java21.data2flow.action.notification.domain;

/**
 * 묶음 규칙(BR-RUL-15·BR-OPS-07, OPS-06.04, TC-RUL-083). 조정한 의미(NFR-01.11 30초 안 발송을 지키기 위해):
 * <ul>
 *   <li>요청·정책 묶기 창(EXPLICIT)·방해 금지(DND): 창 끝까지 모두 기다린다.</li>
 *   <li>채널 기본 묶음(CHANNEL): 창 안 같은 묶음 키의 알림 중 앞 (기준-1)건은 바로 보내고, 기준번째(기본 5번째)부터 모은다. 채널 분당
 *       한도를 넘으면 바로 보내지 않고 모은다(버리지 않음).</li>
 *   <li>창이 끝나면: 기다린 것이 없으면 끝, 1건이면 그대로 보내고, 2건 이상이면 요약 1건.</li>
 * </ul>
 */
public final class DigestRule {

    public enum Flush { NONE, SINGLE, SUMMARY }

    private DigestRule() {
    }

    /** 채널 기본 묶음에서 지금 바로 보낼지 */
    public static boolean sendNow(int sentInWindow, int threshold, boolean overRate) {
        return !overRate && sentInWindow < threshold - 1;
    }

    /** 창을 닫을 때 기다린 건수로 정하는 동작 */
    public static Flush flush(int held) {
        return held <= 0 ? Flush.NONE : held == 1 ? Flush.SINGLE : Flush.SUMMARY;
    }
}
