package com.example.jobrunrdemo;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.springframework.stereotype.Component;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

@CustomLog
@Component
@RequiredArgsConstructor
public class DemoScheduler {

	@Job(name = "데모 Job", labels = {"WMS", "OMS"})
	@Recurring(
		id = "demo-job",
		cron = "0 0 * * * *"
	)
	public void demoJob() throws InterruptedException {
		log.info("데모 Job 실행 시작");
		Thread.sleep(10000);
		log.info("데모 Job 실행 완료");
	}

}
