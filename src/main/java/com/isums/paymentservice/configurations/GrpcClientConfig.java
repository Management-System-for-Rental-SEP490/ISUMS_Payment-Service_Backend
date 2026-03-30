package com.isums.paymentservice.configurations;

import com.isums.assetservice.grpc.AssetServiceGrpc;
import com.isums.houseservice.grpc.HouseServiceGrpc;
import com.isums.houseservice.grpc.TenantServiceGrpc;
import com.isums.userservice.grpc.UserServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GrpcChannelFactory;

@Configuration
public class GrpcClientConfig {

    @Bean
    HouseServiceGrpc.HouseServiceBlockingStub houseStub(GrpcChannelFactory channels, GrpcTokenInterceptor tokenInterceptor) {
        return HouseServiceGrpc.newBlockingStub(channels.createChannel("house"))
                .withInterceptors(tokenInterceptor);
    }

    @Bean
    AssetServiceGrpc.AssetServiceBlockingStub assetStub(GrpcChannelFactory channels, GrpcTokenInterceptor tokenInterceptor) {
        return AssetServiceGrpc.newBlockingStub(channels.createChannel("asset"))
                .withInterceptors(tokenInterceptor);
    }

    @Bean
    UserServiceGrpc.UserServiceBlockingStub userStub(GrpcChannelFactory channels, GrpcTokenInterceptor tokenInterceptor) {
        return UserServiceGrpc.newBlockingStub(channels.createChannel("user"))
                .withInterceptors(tokenInterceptor);
    }

    @Bean
    TenantServiceGrpc.TenantServiceBlockingStub tenantStub(GrpcChannelFactory channels, GrpcTokenInterceptor tokenInterceptor) {
        return TenantServiceGrpc.newBlockingStub(channels.createChannel("tenant"))
                .withInterceptors(tokenInterceptor);
    }

}
