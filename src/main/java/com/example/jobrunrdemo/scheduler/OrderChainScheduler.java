package com.example.jobrunrdemo.scheduler;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.stereotype.Component;

import com.example.jobrunrdemo.common.CompensationDispatcher;

import jakarta.annotation.PostConstruct;
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
 *   <li>체인의 시작점({@link #startChain(JobContext)})은 {@link JobContext}를 주입받아
 *       <b>자신의 실제 Job ID</b>를 얻고, 이를 {@code rootJobId}로 하위 Job에 파라미터로 전달한다.
 *       하위 Job은 자기 자신의 ID만 알 수 있고 부모 ID는 알 수 없으므로, 이렇게 명시적으로 넘겨야
 *       "가장 상위 Job이 누구인지"를 알 수 있다. rootJobId는 체인을 따라 계속 전파된다.</li>
 *   <li>각 하위 Job의 {@code @Job(name)}에는 {@code %0} 플레이스홀더로 rootJobId가 삽입되어,
 *       대시보드에서 같은 rootJobId를 가진 Job들이 <b>어떤 최상위 Job에서 파생된 하위 Job인지</b>
 *       한눈에 구분된다.</li>
 *   <li>마지막 단계가 재시도까지 모두 소진해 <b>최종 실패</b>하면, {@link CompensationDispatcher}에
 *       등록해 둔 보상 핸들러가 실행되어 앞 단계의 효과를 역순으로 되돌리는
 *       <b>보상 처리 체인</b>(saga 패턴)을 enqueue한다.</li>
 * </ul>
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class OrderChainScheduler {

	private final JobScheduler jobScheduler;
	private final CompensationDispatcher compensationDispatcher;

	/**
	 * 출고 지시 단계가 최종 실패하면 보상 처리 체인을 시작하도록 등록한다.
	 * (필터 클래스를 새로 만들지 않고 람다 등록만으로 처리)
	 */
	@PostConstruct
	void registerCompensations() {
		compensationDispatcher.onFinalFailure(
			OrderChainScheduler.class, "issueShipment", failure -> {
				String rootJobId = failure.parameter(0);
				jobScheduler.enqueue(() -> compensateReleaseStock(rootJobId, failure.reason()));
			}
		);
	}

	/**
	 * 체인의 시작점. {@link JobContext}로 <b>자신의 실제 Job ID</b>를 얻어
	 * 이를 rootJobId로 첫 단계 Job에 넘긴다.
	 *
	 * <p>{@code @Recurring} 메서드는 원래 파라미터를 가질 수 없지만, {@link JobContext}만은
	 * JobRunr가 실행 시점에 주입해 주므로 예외적으로 받을 수 있다.
	 */
	@Job(name = "주문 처리 체인 시작(트리거) - 성공 시나리오", labels = {"OMS", "CHAIN"})
	@Recurring(
		id = "order-chain-job-success",
		interval = "PT20M"
	)
	public void startChainWithSuccess(JobContext jobContext) {
		var rootJobId = jobContext.getJobId().toString();
		log.info("주문 처리 체인 시작 - rootJobId={}", rootJobId);
		jobScheduler.enqueue(() -> collectOrders(rootJobId, true));
	}

	@Job(name = "주문 처리 체인 시작(트리거) - 실패 시나리오", labels = {"OMS", "CHAIN"})
	@Recurring(
		id = "order-chain-job-failure",
		interval = "PT20M"
	)
	public void startChainWithFailure(JobContext jobContext) {
		var rootJobId = jobContext.getJobId().toString();
		log.info("주문 처리 체인 시작 - rootJobId={}", rootJobId);
		jobScheduler.enqueue(() -> collectOrders(rootJobId, false));
	}

	/**
	 * 1단계: 주문 수집. 성공 시 재고 확인 단계를 enqueue.
	 */
	@Job(name = "주문 체인[%0] - 1. 주문 수집", labels = {"OMS", "CHAIN"})
	public void collectOrders(String rootJobId, boolean success) throws InterruptedException {
		log.info("[주문 수집] 시작 - rootJobId={}", rootJobId);
		Thread.sleep(2000);
		log.info("[주문 수집] 완료 - 다음 단계(재고 확인) 예약");
		jobScheduler.enqueue(() -> checkStock(rootJobId, success));
	}

	/**
	 * 2단계: 재고 확인. 성공 시 출고 지시 단계를 enqueue.
	 */
	@Job(name = "주문 체인[%0] - 2. 재고 확인", labels = {"OMS", "CHAIN"})
	public void checkStock(String rootJobId, boolean success) throws InterruptedException {
		log.info("[재고 확인] 시작 - rootJobId={}", rootJobId);
		Thread.sleep(2000);
		log.info("[재고 확인] 완료 - 다음 단계(출고 지시) 예약");
		jobScheduler.enqueue(() -> issueShipment(rootJobId, success));
	}

	/**
	 * 3단계: 출고 지시(체인의 마지막). 시작점 시나리오({@code success})에 따라 성공/실패가 결정된다.
	 * 실패 시나리오에서는 항상 예외를 던져 보상 처리 체인을 시연할 수 있다.
	 *
	 * <p>이 단계는 그냥 예외를 던질 뿐, 보상 처리를 직접 호출하지 않는다.
	 * JobRunr가 {@code retries}만큼 재시도하고, <b>모든 재시도가 소진되어 최종 실패</b>하면
	 * {@link #registerCompensations()}에서 {@link CompensationDispatcher}에 등록한 핸들러가
	 * 그 시점을 감지해 보상 처리 체인을 enqueue한다.
	 * (재시도마다 보상이 중복 실행되지 않고, 최종 실패에만 정확히 한 번 실행됨)
	 */
	@Job(name = "주문 체인[%0] - 3. 출고 지시", labels = {"OMS", "CHAIN"}, retries = 2)
	public void issueShipment(String rootJobId, boolean success) throws InterruptedException {
		log.info("[출고 지시] 시작 - rootJobId={}", rootJobId);
		Thread.sleep(2000);
		if (!success) {
			throw new IllegalStateException("출고 시스템 연동 실패 (실패 시나리오 강제 실패)");
		}
		log.info("[출고 지시] 완료 - 주문 처리 체인 종료 rootJobId={}", rootJobId);
	}

	/**
	 * 보상 1단계: 재고 예약 해제(2단계 '재고 확인'의 효과 되돌리기).
	 * 성공 시 다음 보상 단계(주문 취소)를 enqueue한다.
	 */
	@Job(name = "주문 체인[%0] - 보상 1. 재고 예약 해제", labels = {"OMS", "CHAIN", "COMPENSATION"})
	public void compensateReleaseStock(String rootJobId, String reason) throws InterruptedException {
		log.warn("[보상] 재고 예약 해제 - rootJobId={}, 사유={}", rootJobId, reason);
		Thread.sleep(1000);
		// TODO: 실제 재고 예약 해제 로직
		jobScheduler.enqueue(() -> compensateCancelOrders(rootJobId));
	}

	/**
	 * 보상 2단계: 수집한 주문 취소(1단계 '주문 수집'의 효과 되돌리기). 보상 체인의 마지막.
	 */
	@Job(name = "주문 체인[%0] - 보상 2. 주문 취소", labels = {"OMS", "CHAIN", "COMPENSATION"})
	public void compensateCancelOrders(String rootJobId) throws InterruptedException {
		log.warn("[보상] 수집 주문 취소 - rootJobId={}", rootJobId);
		Thread.sleep(1000);
		// TODO: 실제 주문 취소 로직
		log.info("[보상] 완료 - 주문 처리 롤백 종료 rootJobId={}", rootJobId);
	}
}
