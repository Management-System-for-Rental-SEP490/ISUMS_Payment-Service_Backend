package com.isums.paymentservice.infrastructures.grpcs;

import com.isums.houseservice.grpc.GetHouseRequest;
import com.isums.houseservice.grpc.HouseResponse;
import com.isums.houseservice.grpc.HouseServiceGrpc;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class HouseGrpcClient {

    private final HouseServiceGrpc.HouseServiceBlockingStub houseStub;

    public HouseResponse getHouse(UUID houseId) {
        try {
            return houseStub.getHouseById(
                    GetHouseRequest.newBuilder()
                            .setHouseId(houseId.toString())
                            .build());
        } catch (StatusRuntimeException e) {
            log.warn("[HouseGrpcClient] getHouse failed houseId={}: {}", houseId, e.getMessage());
            return null;
        }
    }
}