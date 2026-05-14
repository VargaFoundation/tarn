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

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import varga.tarn.yarn.TarnConfig;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates the happy path and the rejection paths of {@link JwtValidator}. Spins up a
 * tiny HTTP server that serves a JWKS containing our test public key, then signs JWTs
 * with the matching private key and feeds them through the validator.
 */
public class JwtValidatorTest {

    private static final String ISSUER = "https://idp.test.local";
    private static final String AUDIENCE = "tarn";

    private HttpServer jwksServer;
    private int jwksPort;
    private RSAKey signingKey;

    @BeforeEach
    void setup() throws Exception {
        signingKey = new RSAKeyGenerator(2048).keyID("test-key").generate();
        JWKSet jwks = new JWKSet(signingKey.toPublicJWK());

        jwksServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jwksServer.createContext("/jwks.json", ex -> {
            byte[] body = jwks.toString().getBytes();
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        jwksServer.start();
        jwksPort = jwksServer.getAddress().getPort();
    }

    @AfterEach
    void teardown() {
        if (jwksServer != null) jwksServer.stop(0);
    }

    private JwtValidator validator() throws Exception {
        TarnConfig cfg = new TarnConfig();
        cfg.oauthIssuer = ISSUER;
        cfg.oauthAudience = AUDIENCE;
        cfg.oauthJwksUrl = "http://127.0.0.1:" + jwksPort + "/jwks.json";
        cfg.oauthGroupsClaim = "groups";
        return new JwtValidator(cfg);
    }

    private String sign(JWTClaimsSet claims) throws Exception {
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256)
                        .type(JOSEObjectType.JWT)
                        .keyID(signingKey.getKeyID())
                        .build(),
                claims);
        jwt.sign(new RSASSASigner(signingKey));
        return jwt.serialize();
    }

    @Test
    public void validatesWellFormedToken() throws Exception {
        String token = sign(new JWTClaimsSet.Builder()
                .subject("alice")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() + 60_000))
                .issueTime(new Date())
                .claim("groups", List.of("data-platform", "ml-eng"))
                .build());

        var claims = validator().validate(token);
        assertEquals("alice", JwtValidator.userOf(claims));
        assertTrue(validator().groupsOf(claims).contains("data-platform"));
        assertTrue(validator().groupsOf(claims).contains("ml-eng"));
    }

    @Test
    public void rejectsExpiredToken() throws Exception {
        String token = sign(new JWTClaimsSet.Builder()
                .subject("alice")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() - 60_000))
                .issueTime(new Date(System.currentTimeMillis() - 120_000))
                .build());

        assertThrows(Exception.class, () -> validator().validate(token),
                "an expired JWT must be rejected, not silently accepted");
    }

    @Test
    public void rejectsWrongAudience() throws Exception {
        String token = sign(new JWTClaimsSet.Builder()
                .subject("alice")
                .issuer(ISSUER)
                .audience("other-app")
                .expirationTime(new Date(System.currentTimeMillis() + 60_000))
                .issueTime(new Date())
                .build());

        assertThrows(Exception.class, () -> validator().validate(token));
    }

    @Test
    public void rejectsWrongIssuer() throws Exception {
        String token = sign(new JWTClaimsSet.Builder()
                .subject("alice")
                .issuer("https://attacker.example")
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() + 60_000))
                .issueTime(new Date())
                .build());

        assertThrows(Exception.class, () -> validator().validate(token));
    }

    @Test
    public void rejectsTokenSignedByUntrustedKey() throws Exception {
        RSAKey rogue = new RSAKeyGenerator(2048).keyID("rogue").generate();
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("rogue").build(),
                new JWTClaimsSet.Builder()
                        .subject("alice")
                        .issuer(ISSUER)
                        .audience(AUDIENCE)
                        .expirationTime(new Date(System.currentTimeMillis() + 60_000))
                        .issueTime(new Date())
                        .build());
        jwt.sign(new RSASSASigner(rogue));

        assertThrows(Exception.class, () -> validator().validate(jwt.serialize()),
                "JWT signed with a key not in the JWKS must be rejected");
    }
}
