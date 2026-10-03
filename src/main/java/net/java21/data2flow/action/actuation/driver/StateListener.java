package net.java21.data2flow.action.actuation.driver;

/** push 상태 보고 수신(ACT-03.01 "상태 변경 통지") */
@FunctionalInterface
public interface StateListener {

    void onState(long deviceId, ReportedState state);
}
