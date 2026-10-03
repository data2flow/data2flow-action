package net.java21.data2flow.action.actuation.controller;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.service.Outcome;

/** 거부·차단된 명령(기록됨). 4xx 응답의 {@code response.commandId}에 명령 ID를 싣는다(API-ACT-01 참고) */
public class CommandRejectedException extends RuntimeException {

    private final transient Outcome.Rejection rejection;
    private final transient Command command;

    public CommandRejectedException(Outcome.Rejection rejection, Command command) {
        super(rejection.code().code());
        this.rejection = rejection;
        this.command = command;
    }

    public Outcome.Rejection rejection() {
        return rejection;
    }

    public Command command() {
        return command;
    }
}
