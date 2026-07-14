package com.example.jobrunrdemo.common;

import java.util.List;
import java.util.UUID;

/**
 * Job이 최종 실패했을 때 보상 핸들러에게 전달되는 정보.
 *
 * @param jobId      실패한 Job의 ID
 * @param className  실패한 Job의 클래스명
 * @param methodName 실패한 Job의 메서드명
 * @param parameters 실패한 Job이 호출된 인자들(원래 enqueue될 때의 파라미터)
 * @param reason     실패 사유(예외 메시지)
 */
public record JobFailureContext(
	UUID jobId,
	String className,
	String methodName,
	List<Object> parameters,
	String reason
) {

	/**
	 * 인자를 타입 캐스팅해 꺼내는 편의 메서드.
	 */
	@SuppressWarnings("unchecked")
	public <T> T parameter(int index) {
		return (T) parameters.get(index);
	}
}
