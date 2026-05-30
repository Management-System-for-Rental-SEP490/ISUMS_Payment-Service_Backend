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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
public class RemoteRoleJwtConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    private final UserServiceGrpc.UserServiceBlockingStub userStub;

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        List<GrantedAuthority> tokenAuthorities = extractAuthoritiesFromJwt(jwt);
        if (!tokenAuthorities.isEmpty()) {
            return new JwtAuthenticationToken(jwt, tokenAuthorities);
        }

        try {
            UserServiceGrpc.UserServiceBlockingStub stubWithToken = userStub
                    .withCallCredentials(new BearerTokenCallCredentials(jwt.getTokenValue()));

            GetUserRolesResponse response = stubWithToken.getUserRoles(
                    GetUserRolesRequest.newBuilder()
                            .setKeycloakId(jwt.getSubject())
                            .build()
            );

            List<GrantedAuthority> authorities = response.getRolesList().stream()
                    .map(role -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + role))
                    .toList();

            return new JwtAuthenticationToken(jwt, authorities);

        } catch (Exception e) {
            log.warn("Failed to fetch roles for keycloakId={}, defaulting to empty. Error: {}",
                    jwt.getSubject(), e.getMessage());
            return new JwtAuthenticationToken(jwt, List.of());
        }
    }

    private List<GrantedAuthority> extractAuthoritiesFromJwt(Jwt jwt) {
        Set<String> roles = new LinkedHashSet<>();
        addStringRoles(roles, jwt.getClaim("roles"));

        Object realmAccess = jwt.getClaim("realm_access");
        if (realmAccess instanceof Map<?, ?> realmAccessMap) {
            addStringRoles(roles, realmAccessMap.get("roles"));
        }

        Object resourceAccess = jwt.getClaim("resource_access");
        if (resourceAccess instanceof Map<?, ?> resourceAccessMap) {
            resourceAccessMap.values().forEach(clientAccess -> {
                if (clientAccess instanceof Map<?, ?> clientAccessMap) {
                    addStringRoles(roles, clientAccessMap.get("roles"));
                }
            });
        }

        return roles.stream()
                .map(RemoteRoleJwtConverter::normalizeRole)
                .filter(Objects::nonNull)
                .map(SimpleGrantedAuthority::new)
                .map(authority -> (GrantedAuthority) authority)
                .toList();
    }

    private static void addStringRoles(Set<String> roles, Object claimValue) {
        if (claimValue instanceof String role) {
            roles.add(role);
            return;
        }

        if (claimValue instanceof Collection<?> collection) {
            collection.stream()
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .forEach(roles::add);
        }

        if (claimValue instanceof Object[] array) {
            for (Object item : array) {
                if (item instanceof String role) {
                    roles.add(role);
                }
            }
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
