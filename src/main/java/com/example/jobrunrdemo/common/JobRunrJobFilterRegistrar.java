package com.example.jobrunrdemo.common;

import java.util.List;

import org.jobrunr.jobs.filters.JobFilter;
import org.jobrunr.server.BackgroundJobServer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

/**
 * Spring 컨텍스트에 등록된 {@link JobFilter} 빈들을 {@link BackgroundJobServer}에 추가로 등록한다.
 *
 * <p>JobRunr Spring Boot 스타터는 {@code JobFilter} 빈을 자동 등록하지 않고 기본 {@code RetryFilter}만
 * 세팅한다. 이 등록기는 기본 필터(RetryFilter) <b>뒤에</b> 커스텀 필터들을 append하므로,
 * 커스텀 {@code ElectStateFilter}는 RetryFilter가 재시도 여부를 결정한 다음에 실행된다.
 *
 * <p>서버가 기동(ApplicationReadyEvent)하기 전에 실행되도록 {@link SmartInitializingSingleton}을 사용한다.
 */
@Component
@RequiredArgsConstructor
public class JobRunrJobFilterRegistrar implements SmartInitializingSingleton {

	private final ObjectProvider<BackgroundJobServer> backgroundJobServer;
	private final List<JobFilter> jobFilters;

	@Override
	public void afterSingletonsInstantiated() {
		BackgroundJobServer server = backgroundJobServer.getIfAvailable();
		if (server == null || jobFilters.isEmpty()) {
			return;
		}
		server.getJobFilters().addAll(jobFilters);
	}
}
