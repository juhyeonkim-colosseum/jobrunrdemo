package com.example.jobrunrdemo.scheduler;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.filters.ElectStateFilter;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.stereotype.Component;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * 출고 지시 단계가 <b>최종 실패</b>(모든 재시도 소진)했을 때 보상 처리 체인을 시작하는 JobFilter.
 *
 * <p>동작 원리:
 * <ul>
 *   <li>기본 {@code RetryFilter}가 먼저 실행되어, 재시도가 남아 있으면 Job 상태를 다시 SCHEDULED로 되돌린다.</li>
 *   <li>이 필터는 그 뒤에 실행되므로, {@code job.getState()}가 여전히 FAILED이면
 *       재시도가 모두 소진된 <b>최종 실패</b>임을 뜻한다. 이때만 보상을 트리거한다.</li>
 * </ul>
 * (필터 등록은 {@link com.example.jobrunrdemo.common.JobRunrJobFilterRegistrar}가 담당한다.)
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class OrderChainCompensationFilter implements ElectStateFilter {

	private final JobScheduler jobScheduler;
	private final OrderChainScheduler orderChainScheduler;

	@Override
	public void onStateElection(Job job, JobState newState) {
		// RetryFilter가 먼저 실행되어 재시도가 남았으면 SCHEDULED로 되돌린다.
		// 현재 상태가 여전히 FAILED이면 = 재시도 소진(최종 실패)뿐이다.
		if (job.getState() != StateName.FAILED) {
			return;
		}
		JobDetails details = job.getJobDetails();
		if (!isIssueShipment(details)) {
			return;
		}

		String orderBatchId = (String) details.getJobParameters().get(0).getObject();
		String reason = failureReasonOf(job);
		log.error("[체인] 출고 지시 최종 실패 감지 - 보상 처리 체인 시작 batchId={}, 사유={}", orderBatchId, reason);

		// 보상은 정방향의 역순으로 진행한다: 재고 예약 해제 → 주문 취소
		jobScheduler.enqueue(() -> orderChainScheduler.compensateReleaseStock(orderBatchId, reason));
	}

	private boolean isIssueShipment(JobDetails details) {
		return OrderChainScheduler.class.getName().equals(details.getClassName())
			&& "issueShipment".equals(details.getMethodName());
	}

	private String failureReasonOf(Job job) {
		JobState lastState = job.getJobState();
		if (lastState instanceof FailedState failed) {
			return failed.getExceptionMessage();
		}
		return "unknown";
	}
}
