package com.example.jobrunrdemo.common;

/**
 * Job의 단일 단계 실행 단위.
 */
@FunctionalInterface
public interface Step {
	void run() throws InterruptedException;
}
