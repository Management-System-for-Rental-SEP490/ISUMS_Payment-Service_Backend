package com.isums.paymentservice.infrastructures.grpcs;

import com.isums.userservice.grpc.GetUserByIdRequest;
import com.isums.userservice.grpc.GetUserIdAndRoleByKeyCloakIdRequest;
import com.isums.userservice.grpc.UserResponse;
import com.isums.userservice.grpc.UserServiceGrpc;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class UserGrpcService {

    private final UserServiceGrpc.UserServiceBlockingStub userStub;

    public String getTenantEmail(UUID tenantId) {
        try {
            return userStub.getUserById(GetUserByIdRequest.newBuilder().setUserId(tenantId.toString()).build()).getEmail();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to fetch tenant email: " + tenantId);
        }
    }

    public UserResponse getUserIdAndRoleByKeyCloakId(String keycloakId) {
        GetUserIdAndRoleByKeyCloakIdRequest req = GetUserIdAndRoleByKeyCloakIdRequest.newBuilder().setKeycloakId(keycloakId).build();
        return userStub.getUserIdAndRoleByKeyCloakId(req);
    }
}

