package com.example.jobrunrdemo.dbos;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Component;

import dev.dbos.transact.workflow.Step;

import lombok.CustomLog;

/**
 * 주문 처리 워크플로우의 개별 스텝 모음(별도 빈).
 *
 * <p>{@link DbosOrderWorkflowService}가 이 빈을 주입받아 스텝을 호출한다. 스텝을 워크플로우와 다른
 * 빈으로 분리했기 때문에, 워크플로우가 {@code steps.xxx()}로 호출하면 자연스럽게 이 빈의 Spring
 * 프록시를 거쳐 DBOSAspect에 가로채인다. (self-injection 없이 durable 스텝 기록이 가능)
 */
@CustomLog
@Component
public class DbosOrderSteps {

	@Step(name = "주문 수집")
	public void collectOrders(String orderBatchId) {
		log.info("[DBOS][스텝] 주문 수집 batchId={}", orderBatchId);
		sleep(2000);
	}

	@Step(name = "재고 확인")
	public void checkStock(String orderBatchId) {
		log.info("[DBOS][스텝] 재고 확인 batchId={}", orderBatchId);
		sleep(2000);
	}

	/**
	 * 출고 지시. 실패 테스트용으로 90% 확률로 실패한다.
	 * DBOS가 스텝 단위로 최대 3회(지수 백오프) 재시도하고, 그래도 실패하면 예외를 전파한다.
	 */
	@Step(name = "출고 지시", maxAttempts = 3, intervalSeconds = 1, backOffRate = 2)
	public void issueShipment(String orderBatchId) {
		log.info("[DBOS][스텝] 출고 지시 시도 batchId={}", orderBatchId);
		if (ThreadLocalRandom.current().nextDouble() < 0.9) {
			throw new IllegalStateException("출고 시스템 연동 실패 (테스트용 90% 실패)");
		}
		sleep(2000);
	}

	@Step(name = "재고 예약 해제(보상)")
	public void releaseStock(String orderBatchId) {
		log.warn("[DBOS][스텝] 재고 예약 해제(보상) batchId={}", orderBatchId);
		sleep(1000);
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}
}
