package com.isums.paymentservice.exceptions;

import com.isums.paymentservice.domains.dtos.ApiResponse;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.MethodParameter;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("GlobalExceptionHandler (payment-service)")
class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    @DisplayName("handleValidation collects field errors into 400 response")
    void validation() {
        BeanPropertyBindingResult bindingResult = new BeanPropertyBindingResult(new Object(), "req");
        bindingResult.addError(new FieldError("req", "email", "must not be blank"));
        bindingResult.addError(new FieldError("req", "amount", "must be positive"));

        MethodArgumentNotValidException ex = new MethodArgumentNotValidException(
                (MethodParameter) null, bindingResult);

        ResponseEntity<ApiResponse<Void>> res = handler.handleValidation(ex);

        assertThat(res.getStatusCode().value()).isEqualTo(400);
        assertThat(res.getBody().getErrors()).hasSize(2);
        assertThat(res.getBody().getErrors()).allMatch(e -> "VALIDATION_ERROR".equals(e.getCode()));
        assertThat(res.getBody().getMessage()).isEqualTo("Validation failed");
    }

    @Test
    @DisplayName("handleNotFound returns 404 with the exception message")
    void notFound() {
        ResponseEntity<ApiResponse<Void>> res =
                handler.handleNotFound(new EntityNotFoundException("Invoice not found"));
        assertThat(res.getStatusCode().value()).isEqualTo(404);
        assertThat(res.getBody().getMessage()).isEqualTo("Invoice not found");
    }

    @Test
    @DisplayName("handleIllegalArgument returns 400")
    void illegalArg() {
        ResponseEntity<ApiResponse<Void>> res =
                handler.handleIllegalArgument(new IllegalArgumentException("bad arg"));
        assertThat(res.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("handleIllegalState returns 400 with message")
    void illegalState() {
        ResponseEntity<ApiResponse<Void>> res =
                handler.handleIllegalState(new IllegalStateException("Invoice already paid"));
        assertThat(res.getStatusCode().value()).isEqualTo(400);
        assertThat(res.getBody().getMessage()).isEqualTo("Invoice already paid");
    }

    @Test
    @DisplayName("handleAccessDenied returns 403 (security-critical)")
    void accessDenied() {
        ResponseEntity<ApiResponse<Void>> res =
                handler.handleAccessDenied(new AccessDeniedException("denied"));
        assertThat(res.getStatusCode().value()).isEqualTo(403);
    }

    @Test
    @DisplayName("handleGeneral returns 500 but hides internal message from client")
    void general() {
        ResponseEntity<ApiResponse<Void>> res =
                handler.handleGeneral(new Exception("sensitive detail"));
        assertThat(res.getStatusCode().value()).isEqualTo(500);
        assertThat(res.getBody().getMessage()).isEqualTo("Internal server error");
    }
}
