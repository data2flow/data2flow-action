package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.common.CoreClient;

import java.util.Set;

/**
 * 샌드박스 공간 목록(SIM-07.03, BR-ACT-23). core에서 처음 쓸 때 읽고, {@code data2flow.config} SIM_SANDBOX를 받으면 다시 읽는다
 * (1초 안 반영, TC-ACT-030). 한 번도 읽지 못했으면 판단할 수 없으므로 예외를 던져 명령을 다시 시도하게 한다(안전 쪽).
 */
public class SandboxRegistry {

    private final CoreClient core;
    private volatile Set<Long> spaces;

    public SandboxRegistry(CoreClient core) {
        this.core = core;
    }

    public Set<Long> spaces() {
        Set<Long> s = spaces;
        if (s == null) {
            s = reload();
        }
        return s;
    }

    /** core에서 다시 읽는다 */
    public synchronized Set<Long> reload() {
        Set<Long> loaded = Set.copyOf(core.sandboxSpaces());
        spaces = loaded;
        return loaded;
    }

    /** 다음 사용 때 다시 읽게 한다(재연결 등) */
    public void invalidate() {
        spaces = null;
    }
}
