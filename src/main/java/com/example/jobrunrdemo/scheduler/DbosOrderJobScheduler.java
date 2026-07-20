package com.example.jobrunrdemo.scheduler;

import java.util.UUID;

import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.annotations.Recurring;
import org.springframework.stereotype.Component;

import com.example.jobrunrdemo.dbos.DbosOrderWorkflowService;

import dev.dbos.transact.context.WorkflowOptions;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

/**
 * JobRunr + DBOS 조합 샘플.
 *
 * <p>역할 분담:
 * <ul>
 *   <li><b>JobRunr</b> — 스케줄링/진입점. recurring으로 주기 실행되며 대시보드에서 트리거 Job이 보인다.</li>
 *   <li><b>DBOS</b> — 여기서 시작한 {@link DbosOrderWorkflowService#processOrder(String)} 워크플로우가
 *       단계 chaining·재개·보상을 durable하게 담당한다.</li>
 * </ul>
 *
 * <p>워크플로우 ID를 batchId로 고정하므로, JobRunr가 이 트리거 Job을 재시도하더라도 DBOS는 같은
 * 워크플로우로 인식해 <b>중복 실행하지 않고 이어서 재개</b>한다(멱등).
 */
@CustomLog
@Component
@RequiredArgsConstructor
public class DbosOrderJobScheduler {

	private final DbosOrderWorkflowService orderWorkflow;

	@Job(name = "DBOS 주문 워크플로우 트리거", labels = {"DBOS"})
	@Recurring(
		id = "dbos-order-workflow",
		interval = "PT20M"
	)
	public void triggerOrderWorkflow() {
		String orderBatchId = UUID.randomUUID().toString();
		log.info("[JobRunr] DBOS 주문 워크플로우 트리거 batchId={}", orderBatchId);

		// 워크플로우 ID = batchId → JobRunr 재시도 시 같은 DBOS 워크플로우로 재개(중복 방지)
		try (var ignored = new WorkflowOptions(orderBatchId).setContext()) {
			orderWorkflow.processOrder(orderBatchId);
		}
	}
}
