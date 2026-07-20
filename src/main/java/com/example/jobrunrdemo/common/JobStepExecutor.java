package com.example.jobrunrdemo.common;

import java.util.function.Consumer;

import org.jobrunr.jobs.context.JobContext;
import org.springframework.stereotype.Component;

import lombok.CustomLog;

/**
 * 다단계 Job에서 개별 단계를 실행하는 공통 실행기.
 *
 * <p>다음 공통 관심사를 담당한다.
 * <ul>
 *   <li>재시도 안전성: {@link JobContext} metadata에 완료 단계를 기록해,
 *       Job이 재시도되더라도 이미 끝난 단계는 건너뛴다.</li>
 *   <li>대시보드 로깅: 단계의 시작/완료/건너뜀/실패를 기록한다.</li>
 *   <li>실패 처리: 실패 핸들러 호출 후 {@link StepFailedException}으로 감싸 다시 던져
 *       Job을 실패시키고 JobRunr가 재시도하도록 한다.</li>
 * </ul>
 *
 * <p>단계별 비즈니스 로직과 성공/실패 후처리는 호출하는 스케줄러가 제공한다.
 */
@CustomLog
@Component
public class JobStepExecutor {

	/**
	 * 성공/실패 후처리가 필요 없는 단계 실행.
	 */
	public void run(String stepName, Step step) {
		run(
			stepName,
			step,
			() -> {
			},
			e -> {
			}
		);
	}

	/**
	 * 단계를 실행하되, 이미 완료된 단계(metadata에 기록됨)는 건너뛴다.
	 *
	 * <ul>
	 *   <li>성공: 완료 상태를 metadata에 저장(재시도 시 재실행 방지) 후 {@code onSuccess} 실행</li>
	 *   <li>실패: {@code onFailure}로 단계별 대응 후 예외를 다시 던져 Job을 실패시킨다</li>
	 * </ul>
	 */
	public void run(
		String stepName,
		Step step,
		Runnable onSuccess,
		Consumer<Exception> onFailure
	) {
		log.info("[{}] 시작", stepName);
		try {
			step.run();
		} catch (RuntimeException | InterruptedException e) {
			log.error("[{}] 실패", stepName, e);
			onFailure.accept(e);
			throw new StepFailedException(stepName + " 단계 실패", e);
		}

		log.info("[{}] 완료", stepName);
		onSuccess.run();
	}
}
