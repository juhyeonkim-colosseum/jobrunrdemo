package com.example.jobrunrdemo.dbos;

import org.springframework.stereotype.Component;

import dev.dbos.transact.workflow.Workflow;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * DBOS durable workflow로 구현한 주문 처리 체인.
 *
 * <p>{@code OrderChainScheduler}(JobRunr로 단계를 수동 enqueue)와 달리, 여기서는 하나의
 * {@link Workflow @Workflow} 메서드가 스텝들을 순서대로 호출하기만 하면 DBOS가 각 스텝 결과를
 * DB에 체크포인트한다. 프로세스가 중간에 죽어도 <b>마지막 성공 스텝 다음부터 자동 재개</b>된다.
 *
 * <p><b>스텝을 별도 빈({@link DbosOrderSteps})으로 분리한 이유</b>: DBOS는 스텝을 Spring AOP
 * 프록시를 통해 가로챈다. 같은 클래스 안에서 {@code this.step()}으로 부르면 프록시를 우회하므로,
 * 스텝을 다른 빈으로 두고 주입받아 {@code steps.step()}으로 호출한다(프록시 경유 → durable 기록).
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class DbosOrderWorkflowService {

	private final DbosOrderSteps steps;

	/**
	 * 주문 처리 워크플로우. 스텝을 순차 호출(chaining)하며, 마지막 스텝이 실패하면 보상 스텝을 실행한다.
	 * chaining·재개·보상 기록은 모두 DBOS가 담당한다.
	 */
	@Workflow(name = "주문 처리 워크플로우")
	public void processOrder(String orderBatchId) {
		log.info("[DBOS] 주문 처리 워크플로우 시작 batchId={}", orderBatchId);

		steps.collectOrders(orderBatchId);
		steps.checkStock(orderBatchId);
		try {
			steps.issueShipment(orderBatchId);
			log.info("[DBOS] 주문 처리 워크플로우 완료 batchId={}", orderBatchId);
		} catch (Exception e) {
			// 보상도 DBOS 스텝으로 기록 → 재개 시 일관성 유지 (saga 패턴)
			log.error("[DBOS] 출고 지시 최종 실패 - 보상(재고 예약 해제) 실행 batchId={}, 사유={}",
				orderBatchId, e.getMessage());
			steps.releaseStock(orderBatchId);
		}
	}
}
