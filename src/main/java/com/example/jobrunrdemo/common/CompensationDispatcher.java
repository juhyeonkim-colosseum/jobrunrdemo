package com.example.jobrunrdemo.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.JobParameter;
import org.jobrunr.jobs.filters.ElectStateFilter;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.StateName;
import org.springframework.stereotype.Component;

import lombok.CustomLog;

/**
 * "특정 Job이 <b>최종 실패</b>하면 보상 처리를 실행"하는 로직을 한곳에 모은 범용 디스패처.
 *
 * <p>Job마다 {@code ElectStateFilter}를 새로 작성할 필요 없이, 각 스케줄러는 초기화 시점에
 * {@link #onFinalFailure(Class, String, Consumer)}로 보상 핸들러(람다)를 <b>등록만</b> 하면 된다.
 *
 * <p>최종 실패 판정 원리:
 * <ul>
 *   <li>기본 {@code RetryFilter}가 먼저 실행되어, 재시도가 남아 있으면 상태를 다시 SCHEDULED로 되돌린다.</li>
 *   <li>이 필터는 그 뒤에 실행되므로, {@code job.getState()}가 여전히 FAILED이면
 *       재시도가 모두 소진된 <b>최종 실패</b>임을 뜻한다. 이때만 등록된 핸들러를 호출한다.</li>
 * </ul>
 * (JobRunr 필터 등록은 {@link JobRunrJobFilterRegistrar}가 담당한다.)
 */
@CustomLog
@Component
public class CompensationDispatcher implements ElectStateFilter {

	private final Map<String, Consumer<JobFailureContext>> handlers = new ConcurrentHashMap<>();

	/**
	 * 특정 Job(클래스+메서드)이 최종 실패했을 때 실행할 보상 핸들러를 등록한다.
	 *
	 * @param jobClass  대상 Job이 정의된 클래스
	 * @param jobMethod 대상 Job 메서드명
	 * @param handler   최종 실패 시 실행할 로직(보통 보상 Job을 enqueue)
	 */
	public void onFinalFailure(Class<?> jobClass, String jobMethod, Consumer<JobFailureContext> handler) {
		handlers.put(key(jobClass.getName(), jobMethod), handler);
	}

	@Override
	public void onStateElection(Job job, JobState newState) {
		if (job.getState() != StateName.FAILED) {
			return;
		}
		JobDetails details = job.getJobDetails();
		Consumer<JobFailureContext> handler = handlers.get(key(details.getClassName(), details.getMethodName()));
		if (handler == null) {
			return;
		}

		List<Object> parameters = details.getJobParameters().stream()
			.map(JobParameter::getObject)
			.toList();
		JobFailureContext context = new JobFailureContext(
			job.getId(),
			details.getClassName(),
			details.getMethodName(),
			parameters,
			failureReasonOf(job)
		);

		log.error("[보상 디스패처] {}#{} 최종 실패 감지 - 보상 핸들러 실행 (jobId={}, 사유={})",
			details.getClassName(), details.getMethodName(), context.jobId(), context.reason());
		handler.accept(context);
	}

	private String key(String className, String methodName) {
		return className + "#" + methodName;
	}

	private String failureReasonOf(Job job) {
		JobState lastState = job.getJobState();
		if (lastState instanceof FailedState failed) {
			return failed.getExceptionMessage();
		}
		return "unknown";
	}
}
