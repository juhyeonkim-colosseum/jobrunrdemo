package com.example.jobrunrdemo.common;

import org.jobrunr.jobs.context.JobRunrDashboardLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lombok {@code @CustomLog}가 사용하는 로거 팩토리.
 *
 * <p>slf4j 로거를 {@link JobRunrDashboardLogger}로 감싸서 반환하므로,
 * 하나의 로깅 호출로 콘솔/파일 로그와 JobRunr 대시보드에 함께 기록된다.
 * (Job 실행 중일 때만 대시보드에 기록되고, 그 외에는 일반 slf4j 로그로 동작)
 *
 * <p>{@code lombok.config}의 {@code lombok.log.custom.declaration} 설정과 연결된다.
 */
public final class DashboardLoggerFactory {

	private DashboardLoggerFactory() {
	}

	public static Logger getLogger(Class<?> clazz) {
		return new JobRunrDashboardLogger(LoggerFactory.getLogger(clazz));
	}
}
