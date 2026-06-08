package com.isums.paymentservice.configurations;

import com.isums.userservice.grpc.GetUserRolesRequest;
import com.isums.userservice.grpc.GetUserRolesResponse;
import com.isums.userservice.grpc.UserServiceGrpc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class RemoteRoleJwtConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    private final UserServiceGrpc.UserServiceBlockingStub userStub;

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        try {
            UserServiceGrpc.UserServiceBlockingStub stubWithToken = userStub
                    .withCallCredentials(new BearerTokenCallCredentials(jwt.getTokenValue()));

            GetUserRolesResponse response = stubWithToken.getUserRoles(
                    GetUserRolesRequest.newBuilder()
                            .setKeycloakId(jwt.getSubject())
                            .build()
            );

            List<GrantedAuthority> authorities = response.getRolesList().stream()
                    .map(RemoteRoleJwtConverter::normalizeRole)
                    .filter(Objects::nonNull)
                    .map(role -> (GrantedAuthority) new SimpleGrantedAuthority(role))
                    .toList();

            return new JwtAuthenticationToken(jwt, authorities);

        } catch (Exception e) {
            log.warn("Failed to fetch roles for keycloakId={}, defaulting to empty. Error: {}",
                    jwt.getSubject(), e.getMessage());
            return new JwtAuthenticationToken(jwt, List.of());
        }
    }

    private static String normalizeRole(String role) {
        if (role == null || role.isBlank()) {
            return null;
        }

        String normalized = role.trim().toUpperCase();
        if (normalized.startsWith("ROLE_")) {
            return normalized;
        }
        return "ROLE_" + normalized;
    }
}
