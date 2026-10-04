package net.java21.data2flow.action.sink.connector;

/** 연결·쓰기 실패 원인(API-FLW-51 error.kind) */
public enum ErrorKind {
    AUTH, DNS, TLS, TIMEOUT, REFUSED, TARGET, OTHER
}
