package com.example.jobrunrdemo.scheduler;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.jobrunr.jobs.context.JobContext;
import org.springframework.stereotype.Component;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * 여러 단계로 구성된 Job 예시.
 *
 * <p>주문 처리를 (1) 주문 수집 → (2) 재고 확인 → (3) 출고 지시 → (4) 완료 처리
 * 네 단계로 나누어 순차 실행한다.
 *
 * <p>각 단계는 {@code JobContext}의 metadata에 완료 여부를 기록하므로,
 * 중간 단계에서 예외가 발생해 Job이 재시도되더라도 이미 끝난 단계는 건너뛴다.
 * (단계가 멱등하지 않을 때 재실행으로 인한 중복 처리를 방지)
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class OrderFulfillmentScheduler {

	private static final String STEP_DONE = "DONE";

	@Job(name = "주문 처리 Job", labels = {"OMS"})
	@Recurring(
		id = "order-fulfillment-job",
		cron = "0 */1 * * * *"
	)
	public void processOrders(JobContext jobContext) throws InterruptedException {
		log.info("주문 처리 Job 시작");

		runStep(jobContext, "1-collect-orders", "주문 수집", this::collectOrders);
		runStep(jobContext, "2-check-stock", "재고 확인", this::checkStock);
		runStep(jobContext, "3-issue-shipment", "출고 지시", this::issueShipment);
		runStep(jobContext, "4-complete", "완료 처리", this::complete);

		log.info("주문 처리 Job 완료");
	}

	/**
	 * 단계를 실행하되, 이미 완료된 단계(metadata에 기록됨)는 건너뛴다.
	 * 성공하면 완료 상태를 metadata에 저장해 재시도 시 재실행되지 않게 한다.
	 */
	private void runStep(JobContext jobContext, String stepKey, String stepName, Step step)
		throws InterruptedException {
		if (STEP_DONE.equals(jobContext.getMetadata().get(stepKey))) {
			log.info("[{}] 이전 실행에서 이미 완료됨 - 건너뜀", stepName);
			return;
		}

		log.info("[{}] 시작", stepName);
		step.run();
		jobContext.saveMetadata(stepKey, STEP_DONE);
		log.info("[{}] 완료", stepName);
	}

	private void collectOrders() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void checkStock() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void issueShipment() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void complete() throws InterruptedException {
		Thread.sleep(2000);
	}

	@FunctionalInterface
	private interface Step {
		void run() throws InterruptedException;
	}
}
