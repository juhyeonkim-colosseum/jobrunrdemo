package com.example.jobrunrdemo.scheduler;

import java.util.concurrent.ThreadLocalRandom;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.springframework.stereotype.Component;

import com.example.jobrunrdemo.common.JobStepExecutor;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * 여러 단계로 구성된 Job 예시.
 *
 * <p>주문 처리를 (1) 주문 수집 → (2) 재고 확인 → (3) 출고 지시
 * 세 단계로 나누어 순차 실행한다. 단계 실행의 공통 관심사(재시도 안전성,
 * 대시보드 로깅, 실패 처리)는 {@link JobStepExecutor}가 담당하고,
 * 이 클래스는 단계별 비즈니스 로직과 성공/실패 후처리만 정의한다.
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class OrderFulfillmentScheduler {

	private final JobStepExecutor stepExecutor;

	@Job(name = "주문 처리(람다 기반 Chaining)", labels = {"OMS"})
	@Recurring(
		id = "order-fulfillment-job",
		interval = "PT20M"
	)
	public void processOrders() {
		log.info("주문 처리 Job 시작");

		// 주문 수집: 성공/실패 시 별도 후처리 없이 진행 (실패하면 예외로 재시도)
		stepExecutor.run("주문 수집", this::collectOrders);

		// 재고 확인: 실패 시 품절 알림
		stepExecutor.run(
			"재고 확인",
			this::checkStock,
			() -> log.info("[재고 확인] 성공 - 출고 진행 가능"),
			e -> log.warn("[재고 확인] 실패 - 품절 알림 발송: {}", e.getMessage())
		);

		// 출고 지시: 성공 시 배송 추적 시작 + 주문 완료 알림, 실패 시 보상 처리(재고 예약 해제)
		stepExecutor.run(
			"출고 지시",
			this::issueShipment,
			() -> log.info("[출고 지시] 성공 - 배송 추적 시작 및 주문 완료 알림 발송"),
			this::compensateShipment
		);

		log.info("주문 처리 Job 완료");
	}

	private void compensateShipment(Exception cause) {
		log.warn("[출고 지시] 실패 - 재고 예약 해제(보상 처리) 수행: {}", cause.getMessage());
	}

	private void collectOrders() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void checkStock() throws InterruptedException {
		Thread.sleep(2000);
	}

	private void issueShipment() throws InterruptedException {
		// 실패 테스트용: 90% 확률로 실패시킨다.
		if (ThreadLocalRandom.current().nextDouble() < 0.9) {
			throw new IllegalStateException("출고 시스템 연동 실패 (테스트용 90% 실패)");
		}
		Thread.sleep(2000);
	}

}
