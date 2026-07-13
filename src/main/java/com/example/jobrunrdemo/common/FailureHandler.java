package com.example.jobrunrdemo.common;

/**
 * 단계 실패 시 호출되는 후처리 핸들러.
 */
@FunctionalInterface
public interface FailureHandler {
	void handle(Exception e);
}
