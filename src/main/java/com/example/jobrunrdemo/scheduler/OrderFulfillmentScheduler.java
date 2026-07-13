package com.example.jobrunrdemo.scheduler;

import java.util.concurrent.ThreadLocalRandom;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.jobrunr.jobs.context.JobContext;
import org.springframework.stereotype.Component;

import com.example.jobrunrdemo.StepFailedException;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * 여러 단계로 구성된 Job 예시.
 *
 * <p>주문 처리를 (1) 주문 수집 → (2) 재고 확인 → (3) 출고 지시
 * 세 단계로 나누어 순차 실행한다.
 *
 * <p>각 단계는 {@code JobContext}의 metadata에 완료 여부를 기록하므로,
 * 중간 단계에서 예외가 발생해 Job이 재시도되더라도 이미 끝난 단계는 건너뛴다.
 * (단계가 멱등하지 않을 때 재실행으로 인한 중복 처리를 방지)
 *
 * <p>또한 단계별로 성공 핸들러 / 실패 핸들러를 지정해, 특정 단계가 성공하거나
 * 실패했을 때 서로 다른 후처리(알림, 보상 처리 등)가 이루어지도록 한다.
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
	public void processOrders(JobContext jobContext) {
		log.info("주문 처리 Job 시작");

		// 주문 수집: 성공/실패 시 별도 후처리 없이 진행 (실패하면 예외로 재시도)
		runStep(jobContext, "1-collect-orders", "주문 수집", this::collectOrders);

		// 재고 확인: 실패 시 품절 알림
		runStep(
			jobContext, "2-check-stock", "재고 확인", this::checkStock,
			() -> log.info("[재고 확인] 성공 - 출고 진행 가능"),
			e -> log.warn("[재고 확인] 실패 - 품절 알림 발송: {}", e.getMessage())
		);

		// 출고 지시: 성공 시 배송 추적 시작 + 주문 완료 알림, 실패 시 보상 처리(재고 예약 해제)
		runStep(
			jobContext, "3-issue-shipment", "출고 지시", this::issueShipment,
			() -> log.info("[출고 지시] 성공 - 배송 추적 시작 및 주문 완료 알림 발송"),
			e -> compensateShipment(e)
		);

		log.info("주문 처리 Job 완료");
	}

	/**
	 * 성공/실패 후처리가 필요 없는 단계용 오버로드.
	 */
	private void runStep(JobContext jobContext, String stepKey, String stepName, Step step) {
		runStep(
			jobContext, stepKey, stepName, step, () -> {
			}, e -> {
			}
		);
	}

	/**
	 * 단계를 실행하되, 이미 완료된 단계(metadata에 기록됨)는 건너뛴다.
	 *
	 * <p>실행 결과에 따라 단계별 후처리를 호출한다.
	 * <ul>
	 *   <li>성공: 완료 상태를 metadata에 저장(재시도 시 재실행 방지) 후 {@code onSuccess} 실행</li>
	 *   <li>실패: {@code onFailure}로 단계별 대응 후 예외를 다시 던져 Job을 실패시킨다
	 *       (JobRunr가 재시도하며, 완료된 이전 단계는 metadata 덕분에 건너뛴다)</li>
	 * </ul>
	 */
	private void runStep(
		JobContext jobContext, String stepKey, String stepName, Step step,
		Runnable onSuccess, FailureHandler onFailure
	) {
		if (STEP_DONE.equals(jobContext.getMetadata().get(stepKey))) {
			log.info("[{}] 이전 실행에서 이미 완료됨 - 건너뜀", stepName);
			return;
		}

		log.info("[{}] 시작", stepName);
		try {
			step.run();
		} catch (Exception e) {
			log.error("[{}] 실패", stepName, e);
			onFailure.handle(e);
			throw new StepFailedException(stepName + " 단계 실패", e);
		}

		jobContext.saveMetadata(stepKey, STEP_DONE);
		log.info("[{}] 완료", stepName);
		onSuccess.run();
	}

	private void compensateShipment(Exception cause) {
		log.warn("[출고 지시] 실패 - 재고 예약 해제(보상 처리) 수행: {}", cause.getMessage());
		// TODO: 재고 예약 해제 등 보상 로직
	}

	private void collectOrders() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void checkStock() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void issueShipment() throws InterruptedException {
		Thread.sleep(2000);
		if (ThreadLocalRandom.current().nextDouble() < 0.9) {
			throw new IllegalStateException("출고 시스템 연동 실패 (테스트용 90% 실패)");
		}
	}

}
