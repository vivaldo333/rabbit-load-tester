package org.ks;

import org.ks.dto.ProcessingResultDto;
import org.ks.dto.PromiseDto;

import java.util.concurrent.CompletableFuture;

public class MessageMapper {
    public PromiseDto map(String message) {
        CompletableFuture<ProcessingResultDto> futureResult = new CompletableFuture<>();
        return new PromiseDto(message, futureResult);
    }

    public ProcessingResultDto map(String message, boolean isSuccessResult) {
        return new ProcessingResultDto(message, isSuccessResult);
    }
}
