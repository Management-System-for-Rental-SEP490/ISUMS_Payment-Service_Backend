package com.isums.paymentservice.domains.dtos;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    Integer statusCode;
    Boolean success;
    String message;
    List<ApiError> errors;
    T data;
}
