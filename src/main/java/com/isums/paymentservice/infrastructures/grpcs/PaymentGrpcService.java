package com.isums.paymentservice.infrastructures.grpcs;

import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.grpc.InvoiceStatusRequest;
import com.isums.paymentservice.grpc.InvoiceStatusResponse;
import com.isums.paymentservice.grpc.PaymentServiceGrpc;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentGrpcService extends PaymentServiceGrpc.PaymentServiceImplBase {

    private final RentalInvoiceRepository invoiceRepository;

    @Override
    public void getInvoiceStatus(InvoiceStatusRequest request, StreamObserver<InvoiceStatusResponse> observer) {
        try {
            UUID houseId = UUID.fromString(request.getHouseId());
            UUID tenantId = UUID.fromString(request.getTenantId());

            boolean depositPaid = invoiceRepository.existsByHouseIdAndTenantIdAndTypeAndStatus(houseId, tenantId, InvoiceType.DEPOSIT, InvoiceStatus.PAID);

            boolean firstRentPaid = invoiceRepository.existsByHouseIdAndTenantIdAndTypeAndStatus(houseId, tenantId, InvoiceType.MONTHLY_RENT, InvoiceStatus.PAID);

            String pendingId = invoiceRepository.findFirstByHouseIdAndTenantIdAndStatus(houseId, tenantId, InvoiceStatus.UNPAID)
                    .map(i -> i.getId().toString()).orElse("");

            observer.onNext(InvoiceStatusResponse.newBuilder()
                    .setDepositPaid(depositPaid)
                    .setFirstRentPaid(firstRentPaid)
                    .setPendingInvoiceId(pendingId)
                    .build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }
}