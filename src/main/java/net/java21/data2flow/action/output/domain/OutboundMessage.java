package net.java21.data2flow.action.output.domain;

/**
 * 보낼 메시지 하나(대기열 행 하나). MQTT면 토픽, Webhook이면 배치에 넣을 항목 본문.
 *
 * @param part 측정값마다 나눠 보낼 때 측정 항목 키, 아니면 빈 문자열(멱등 키의 일부)
 */
public record OutboundMessage(String part, String topic, String body) {
}
