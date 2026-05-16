package com.isums.paymentservice.infrastructures.grpcs;

import com.isums.houseservice.grpc.GetAllHousesRequest;
import com.isums.houseservice.grpc.GetHouseByUserRequest;
import com.isums.houseservice.grpc.GetHouseRequest;
import com.isums.houseservice.grpc.HouseResponse;
import com.isums.houseservice.grpc.HouseServiceGrpc;
import com.isums.houseservice.grpc.ListHouseResponse;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
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

    public List<HouseResponse> getAllHouses() {
        try {
            ListHouseResponse response = houseStub.getAllHouses(GetAllHousesRequest.newBuilder().build());
            return response.getHouseList();
        } catch (StatusRuntimeException e) {
            log.warn("[HouseGrpcClient] getAllHouses failed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<HouseResponse> getHousesByManagerRegion(UUID managerId) {
        try {
            ListHouseResponse response = houseStub.getHousesByManagerRegion(
                    GetHouseByUserRequest.newBuilder()
                            .setUserId(managerId.toString())
                            .build());
            return response.getHouseList();
        } catch (StatusRuntimeException e) {
            log.warn("[HouseGrpcClient] getHousesByManagerRegion failed managerId={}: {}",
                    managerId, e.getMessage());
            return Collections.emptyList();
        }
    }
}