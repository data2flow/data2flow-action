package net.java21.data2flow.action.actuation.driver.lorawan;

import java.util.Map;
import java.util.Set;

/**
 * 표준 명령 → LoRaWAN 다운링크(fPort + 바이트) 인코딩(ACT-03.03, TC-ACT-073). 기본 코덱 {@code data2flow-v1}은 첫 바이트가 기능 번호이고
 * 값이 없는 칸은 0xFF다. 모델별 인코딩 스크립트(driver_bindings.encoder_script_ref)는 아직 쓰지 않는다(ADR-049 남은 것).
 *
 * <table>
 *   <tr><th>기능</th><th>fPort</th><th>바이트</th></tr>
 *   <tr><td>Switch</td><td>10</td><td>01, on(1)/off(0)</td></tr>
 *   <tr><td>Thermostat</td><td>11</td><td>02, mode(off0 cool1 heat2 dry3 fan4 auto5), 목표 온도 × 2</td></tr>
 *   <tr><td>Dimmer</td><td>12</td><td>03, level(0~100)</td></tr>
 *   <tr><td>FanSpeed</td><td>13</td><td>04, level, auto(1/0)</td></tr>
 *   <tr><td>Ventilation</td><td>14</td><td>05, mode(off0 on1 auto2), level</td></tr>
 *   <tr><td>Lock</td><td>15</td><td>06, locked(1)/unlocked(0)</td></tr>
 * </table>
 */
public final class DownlinkEncoder {

    /** 지원 기능 */
    public static final Set<String> CAPABILITIES = Set.of("Switch", "Thermostat", "Dimmer", "FanSpeed", "Ventilation", "Lock");
    private static final int NONE = 0xFF;

    /**
     * 인코딩 결과.
     *
     * @param fPort LoRaWAN 포트
     * @param bytes 페이로드
     */
    public record Downlink(int fPort, byte[] bytes) {
    }

    private DownlinkEncoder() {
    }

    /**
     * @param fPortOverride 드라이버 설정 {@code fPortDefault}(있으면 모든 기능에 그 포트)
     * @throws IllegalArgumentException 지원하지 않는 기능·명령·값(CAPABILITY_NOT_SUPPORTED)
     */
    public static Downlink encode(String capability, String command, Map<String, ?> args, Integer fPortOverride) {
        if (!"set".equals(command)) {
            throw new IllegalArgumentException("CAPABILITY_NOT_SUPPORTED: " + capability + "." + command);
        }
        Downlink d = switch (capability == null ? "" : capability) {
            case "Switch" -> new Downlink(10, new byte[]{0x01, bool(args.get("on"))});
            case "Thermostat" -> new Downlink(11, new byte[]{0x02, code(args.get("mode"), "off", "cool", "heat", "dry", "fan", "auto"),
                    halfDegrees(args.get("targetTemperature"))});
            case "Dimmer" -> new Downlink(12, new byte[]{0x03, number(args.get("level"), 100)});
            case "FanSpeed" -> new Downlink(13, new byte[]{0x04, number(args.get("level"), 254), bool(args.get("auto"))});
            case "Ventilation" -> new Downlink(14, new byte[]{0x05, code(args.get("mode"), "off", "on", "auto"), number(args.get("level"), 254)});
            case "Lock" -> new Downlink(15, new byte[]{0x06, bool(args.get("locked"))});
            default -> throw new IllegalArgumentException("CAPABILITY_NOT_SUPPORTED: " + capability);
        };
        return fPortOverride == null || fPortOverride < 1 ? d : new Downlink(fPortOverride, d.bytes());
    }

    private static byte bool(Object v) {
        if (v == null) {
            return (byte) NONE;
        }
        if (v instanceof Boolean b) {
            return (byte) (b ? 1 : 0);
        }
        throw new IllegalArgumentException("CAPABILITY_NOT_SUPPORTED: boolean 값이 아닙니다: " + v);
    }

    private static byte code(Object v, String... values) {
        if (v == null) {
            return (byte) NONE;
        }
        for (int i = 0; i < values.length; i++) {
            if (values[i].equals(v.toString())) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("CAPABILITY_NOT_SUPPORTED: 모르는 값입니다: " + v);
    }

    private static byte number(Object v, int max) {
        if (v == null) {
            return (byte) NONE;
        }
        if (v instanceof Number n && n.doubleValue() >= 0 && n.doubleValue() <= max) {
            return (byte) n.intValue();
        }
        throw new IllegalArgumentException("CAPABILITY_NOT_SUPPORTED: 범위 밖 값입니다: " + v);
    }

    private static byte halfDegrees(Object v) {
        if (v == null) {
            return (byte) NONE;
        }
        if (v instanceof Number n && n.doubleValue() >= 0 && n.doubleValue() * 2 <= 254) {
            return (byte) Math.round(n.doubleValue() * 2);
        }
        throw new IllegalArgumentException("CAPABILITY_NOT_SUPPORTED: 범위 밖 온도입니다: " + v);
    }
}
