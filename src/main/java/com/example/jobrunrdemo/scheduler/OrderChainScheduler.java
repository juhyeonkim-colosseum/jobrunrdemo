package com.example.jobrunrdemo.scheduler;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.stereotype.Component;

import com.example.jobrunrdemo.common.StepFailedException;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * Job Chaining 예시 (오픈소스 버전 방식).
 *
 * <p>{@link OrderFulfillmentScheduler}가 한 Job 안에서 세 단계를 순차 실행하는 것과 달리,
 * 여기서는 각 단계를 <b>독립된 Job</b>으로 만들고 "현재 단계가 성공하면 다음 단계를 enqueue"하는
 * 방식으로 체인을 구성한다. (JobRunr Pro의 {@code continueWith(...)}를 수동으로 구현한 형태)
 *
 * <p>이 방식의 특징:
 * <ul>
 *   <li>단계마다 별도 Job → 대시보드에서 개별 추적되고, <b>단계별로 독립 재시도</b>된다.</li>
 *   <li>다음 단계 enqueue가 메서드 끝에 있으므로, 중간에 예외가 나면 다음 단계는 큐잉되지 않는다.
 *       실패한 그 Job만 재시도되고, 성공하면 그때 다음 단계로 이어진다.</li>
 *   <li>단계 간 데이터는 파라미터로 전달한다(여기서는 orderBatchId).</li>
 *   <li>마지막 단계가 실패하면 앞 단계의 효과를 역순으로 되돌리는 <b>보상 처리 체인</b>
 *       (saga 패턴)을 enqueue한다.</li>
 * </ul>
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class OrderChainScheduler {

	private final JobScheduler jobScheduler;

	/**
	 * 체인의 시작점. 배치 ID를 생성하고 첫 단계 Job을 enqueue한다.
	 */
	@Job(name = "주문 체인 - 시작", labels = {"OMS", "CHAIN"})
	@Recurring(
		id = "order-chain-job",
		interval = "PT5M"
	)
	public void startChain() {
		String orderBatchId = UUID.randomUUID().toString();
		log.info("주문 처리 체인 시작 - batchId={}", orderBatchId);
		jobScheduler.enqueue(() -> collectOrders(orderBatchId));
	}

	/**
	 * 1단계: 주문 수집. 성공 시 재고 확인 단계를 enqueue.
	 */
	@Job(name = "주문 체인 - 1. 주문 수집", labels = {"OMS", "CHAIN"})
	public void collectOrders(String orderBatchId) throws InterruptedException {
		log.info("[주문 수집] 시작 - batchId={}", orderBatchId);
		Thread.sleep(2000);
		log.info("[주문 수집] 완료 - 다음 단계(재고 확인) 예약");
		jobScheduler.enqueue(() -> checkStock(orderBatchId));
	}

	/**
	 * 2단계: 재고 확인. 성공 시 출고 지시 단계를 enqueue.
	 */
	@Job(name = "주문 체인 - 2. 재고 확인", labels = {"OMS", "CHAIN"})
	public void checkStock(String orderBatchId) throws InterruptedException {
		log.info("[재고 확인] 시작 - batchId={}", orderBatchId);
		Thread.sleep(2000);
		log.info("[재고 확인] 완료 - 다음 단계(출고 지시) 예약");
		jobScheduler.enqueue(() -> issueShipment(orderBatchId));
	}

	/**
	 * 3단계: 출고 지시(체인의 마지막). 실패 테스트용으로 90% 확률로 실패한다.
	 *
	 * <p>실패하면 앞 단계(주문 수집·재고 확인)의 효과를 되돌리는 <b>보상 처리 체인</b>을
	 * enqueue한 뒤 예외를 던져 이 Job을 실패로 남긴다.
	 *
	 * <p>보상이 정확히 한 번만 실행되도록 이 단계는 {@code retries = 0}(재시도 없음)으로 둔다.
	 * 재시도를 유지한 채 "모든 재시도가 소진된 최종 실패"에만 보상하려면, catch에서 직접 enqueue하는
	 * 대신 {@code ElectStateFilter}(JobFilter)로 최종 {@code FailedState} 전이를 감지해 enqueue하면 된다.
	 */
	@Job(name = "주문 체인 - 3. 출고 지시", labels = {"OMS", "CHAIN"}, retries = 0)
	public void issueShipment(String orderBatchId) throws InterruptedException {
		log.info("[출고 지시] 시작 - batchId={}", orderBatchId);
		Thread.sleep(2000);
		try {
			if (ThreadLocalRandom.current().nextDouble() < 0.9) {
				throw new IllegalStateException("출고 시스템 연동 실패 (테스트용 90% 실패)");
			}
		} catch (Exception e) {
			log.error("[출고 지시] 실패 - 보상 처리 체인 시작 batchId={}, 사유={}", orderBatchId, e.getMessage());
			// 보상은 정방향의 역순으로 진행한다: 재고 예약 해제 → 주문 취소
			jobScheduler.enqueue(() -> compensateReleaseStock(orderBatchId, e.getMessage()));
			throw new StepFailedException("출고 지시 단계 실패", e);
		}
		log.info("[출고 지시] 완료 - 주문 처리 체인 종료 batchId={}", orderBatchId);
	}

	/**
	 * 보상 1단계: 재고 예약 해제(2단계 '재고 확인'의 효과 되돌리기).
	 * 성공 시 다음 보상 단계(주문 취소)를 enqueue한다.
	 */
	@Job(name = "주문 체인 - 보상 1. 재고 예약 해제", labels = {"OMS", "COMPENSATION"})
	public void compensateReleaseStock(String orderBatchId, String reason) throws InterruptedException {
		log.warn("[보상] 재고 예약 해제 - batchId={}, 사유={}", orderBatchId, reason);
		Thread.sleep(1000);
		// TODO: 실제 재고 예약 해제 로직
		jobScheduler.enqueue(() -> compensateCancelOrders(orderBatchId));
	}

	/**
	 * 보상 2단계: 수집한 주문 취소(1단계 '주문 수집'의 효과 되돌리기). 보상 체인의 마지막.
	 */
	@Job(name = "주문 체인 - 보상 2. 주문 취소", labels = {"OMS", "COMPENSATION"})
	public void compensateCancelOrders(String orderBatchId) throws InterruptedException {
		log.warn("[보상] 수집 주문 취소 - batchId={}", orderBatchId);
		Thread.sleep(1000);
		// TODO: 실제 주문 취소 로직
		log.info("[보상] 완료 - 주문 처리 롤백 종료 batchId={}", orderBatchId);
	}
}
