package varga.tarn.yarn.auth;

/*-
 * #%L
 * Tarn
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import varga.tarn.yarn.TarnConfig;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates incoming Bearer JWTs against a configured OIDC issuer. The JWKS is fetched
 * once at construction and refreshed by the Nimbus JWKSource on rotation (signature
 * verification falls back to a JWKS reload on unknown {@code kid}).
 *
 * <p>This class is intentionally thin: it answers "is this token well-formed, signed by
 * the right IdP, addressed to us, and not expired?". The caller (DiscoveryServer /
 * OpenAIProxyHandler) then extracts {@code sub} and the configured groups claim and feeds
 * them to Ranger. There is no claim translation here — keep the policy decisions in the
 * authorizer, not the authenticator.
 */
public final class JwtValidator {

    private static final Logger log = LoggerFactory.getLogger(JwtValidator.class);

    private final ConfigurableJWTProcessor<SecurityContext> processor;
    private final String groupsClaimPath;

    public JwtValidator(TarnConfig cfg) throws Exception {
        if (cfg.oauthJwksUrl == null || cfg.oauthJwksUrl.isEmpty()) {
            throw new IllegalStateException("OAuth enabled but oauthJwksUrl is missing");
        }
        // JWKSourceBuilder applies sensible defaults: 5 min refresh interval, 30 s cache TTL
        // for failed fetches, retry on unknown kid. Good enough for our use case — operators
        // who need different cadences can override via JVM properties Nimbus reads.
        JWKSource<SecurityContext> jwkSource = JWKSourceBuilder
                .create(new URL(cfg.oauthJwksUrl))
                .build();

        DefaultJWTProcessor<SecurityContext> p = new DefaultJWTProcessor<>();
        // RS256 is the OIDC baseline; ES256 covers the keys produced by modern IdPs that
        // prefer EC for shorter tokens. Keep the set tight — we don't want to accept "none"
        // or HS256 which would let a leaked symmetric secret forge tokens.
        Set<JWSAlgorithm> allowed = new HashSet<>();
        Collections.addAll(allowed, JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512,
                JWSAlgorithm.ES256, JWSAlgorithm.ES384, JWSAlgorithm.ES512);
        p.setJWSKeySelector(new JWSVerificationKeySelector<>(allowed, jwkSource));

        // Required claims: iss must equal configured issuer, aud must contain the audience.
        // Nimbus also validates exp/nbf with a small clock skew by default (≈30s).
        DefaultJWTClaimsVerifier<SecurityContext> verifier = new DefaultJWTClaimsVerifier<>(
                cfg.oauthAudience,
                new JWTClaimsSet.Builder().issuer(cfg.oauthIssuer).build(),
                new HashSet<>(java.util.Arrays.asList("sub", "iat", "exp")));
        p.setJWTClaimsSetVerifier(verifier);
        this.processor = p;
        this.groupsClaimPath = cfg.oauthGroupsClaim;
    }

    /**
     * Validates and returns the parsed claims. Throws on any failure (bad signature,
     * expired, wrong audience, malformed). Callers map the exception to 401.
     */
    public JWTClaimsSet validate(String bearer) throws Exception {
        SignedJWT jwt = SignedJWT.parse(bearer);
        return processor.process(jwt, null);
    }

    /** Extracts the user identifier the rest of TARN uses — {@code sub} is the OIDC canonical. */
    public static String userOf(JWTClaimsSet claims) {
        return claims.getSubject();
    }

    /**
     * Looks up the configured groups claim with dotted-path support
     * (e.g. {@code realm_access.roles} for Keycloak). Returns an empty set if the claim is
     * missing or the wrong shape — Ranger will then evaluate user-level policies only.
     */
    @SuppressWarnings("unchecked")
    public Set<String> groupsOf(JWTClaimsSet claims) {
        if (groupsClaimPath == null || groupsClaimPath.isEmpty()) {
            return Collections.emptySet();
        }
        Object current = null;
        try {
            String[] parts = groupsClaimPath.split("\\.");
            current = claims.getClaim(parts[0]);
            for (int i = 1; i < parts.length && current != null; i++) {
                if (current instanceof java.util.Map) {
                    current = ((java.util.Map<String, Object>) current).get(parts[i]);
                } else {
                    return Collections.emptySet();
                }
            }
        } catch (Exception e) {
            log.debug("Groups claim extraction failed for {}: {}", groupsClaimPath, e.getMessage());
            return Collections.emptySet();
        }
        if (current instanceof List) {
            Set<String> out = new HashSet<>();
            for (Object o : (List<Object>) current) {
                if (o != null) out.add(String.valueOf(o));
            }
            return out;
        }
        if (current instanceof String) {
            // Some IdPs emit a comma-separated string. Split defensively.
            Set<String> out = new HashSet<>();
            for (String s : ((String) current).split(",")) {
                String t = s.trim();
                if (!t.isEmpty()) out.add(t);
            }
            return out;
        }
        return Collections.emptySet();
    }
}
