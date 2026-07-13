package com.example.jobrunrdemo.scheduler;

/**
 * Job의 단일 단계 실행 단위.
 */
@FunctionalInterface
public interface Step {
	void run() throws InterruptedException;
}
