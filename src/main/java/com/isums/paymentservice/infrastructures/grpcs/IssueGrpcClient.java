package com.isums.paymentservice.infrastructures.grpcs;

import com.isums.issueservice.grpc.GetQuoteByIdRequest;
import com.isums.issueservice.grpc.IssueServiceGrpc;
import com.isums.issueservice.grpc.QuoteResponse;
import com.isums.paymentservice.domains.dtos.IssueQuoteResponse;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class IssueGrpcClient {

    private final IssueServiceGrpc.IssueServiceBlockingStub issueStub;

    public IssueQuoteResponse getQuote(UUID quoteId) {
        try {
            QuoteResponse response = issueStub.getQuoteById(
                    GetQuoteByIdRequest.newBuilder()
                            .setQuoteId(quoteId.toString())
                            .build());

            return new IssueQuoteResponse(
                    UUID.fromString(response.getId()),
                    response.getIssueId().isBlank() ? null : UUID.fromString(response.getIssueId()),
                    response.getTenantId().isBlank() ? null : UUID.fromString(response.getTenantId()),
                    new BigDecimal(response.getTotalPrice()),
                    response.getStatus()
            );

        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                throw new IllegalArgumentException("Báo giá không tồn tại: " + quoteId);
            }
            log.error("[IssueGrpcClient] getQuote failed quoteId={}: {}", quoteId, e.getMessage());
            throw new IllegalStateException("Unable to retrieve price information: " + e.getMessage(), e);
        }
    }
}