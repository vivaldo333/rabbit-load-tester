package org.ks.dto;

import java.util.concurrent.CompletableFuture;

public class PromiseDto {
    private String message;
    private CompletableFuture<ProcessingResultDto> result;

    public PromiseDto(String message, CompletableFuture<ProcessingResultDto> result) {
        this.message = message;
        this.result = result;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public CompletableFuture<ProcessingResultDto> getResult() {
        return result;
    }

    public void setResult(CompletableFuture<ProcessingResultDto> result) {
        this.result = result;
    }
}
