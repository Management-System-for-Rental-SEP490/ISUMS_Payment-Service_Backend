package com.isums.paymentservice.domains.dtos;

import org.springframework.http.HttpStatus;

import java.util.List;

public final class ApiResponses {
    private ApiResponses() {}

    public static <T> ApiResponse<T> ok(T data, String message) {
        return ApiResponse.<T>builder()
                .statusCode(HttpStatus.OK.value())
                .success(true)
                .message(message)
                .data(data)
                .build();
    }

    public static <T> ApiResponse<T> created(T data, String message) {
        return ApiResponse.<T>builder()
                .statusCode(HttpStatus.CREATED.value())
                .success(true)
                .message(message)
                .data(data)
                .build();
    }

    public static <T> ApiResponse<T> fail(HttpStatus status, String message, List<ApiError> errors) {
        return ApiResponse.<T>builder()
                .statusCode(status.value())
                .success(false)
                .message(message)
                .errors(errors)
                .build();
    }

    public static <T> ApiResponse<T> fail(HttpStatus status, String message) {
        return fail(status, message, null);
    }
}

