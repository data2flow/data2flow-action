package net.java21.data2flow.action.common;

import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.authz.CachingPermissionLookup;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.authz.PermissionLookup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** IAM-05.01·IAM-04.07: action은 장기 토큰 요청의 권한을 core access-grant ?accessTokenId=로 묻고, 웹 신원과 다른 캐시 칸에 둔다 */
class CoreClientAccessGrantTest {

    private final FakeCore core = new FakeCore();

    @AfterEach
    void stop() throws IOException {
        core.server.shutdown();
    }

    @Test
    @DisplayName("[IAM-05.01][IAM-04.07] 같은 사용자 웹(OPERATOR) → 토큰(VIEWER) → 웹: 토큰은 제어 권한을 물려받지 않고, 웹은 토큰 판정에 끌려가지 않음")
    void tokenAndWebAreSeparate() {
        // given
        core.roles.put(7L, "OPERATOR");
        core.tokenRoles.put(501L, "VIEWER");
        CoreClient client = new CoreClient(RestClient.builder().baseUrl(core.url()).build());
        PermissionLookup lookup = new CachingPermissionLookup(PermissionLookup.tokenAware(client::accessGrant), Duration.ofSeconds(10),
                new MutableClock(MutableClock.T0));
        // when & then
        assertThat(lookup.find(1, 7).has(Permission.DEVICE_CONTROL)).isTrue();
        assertThat(lookup.find(1, 7, 501L).has(Permission.DEVICE_CONTROL)).isFalse();
        assertThat(lookup.find(1, 7, null).has(Permission.DEVICE_CONTROL)).isTrue();
        assertThat(core.requests).containsExactly("GET /internal/core/organizations/1/users/7/access-grant",
                "GET /internal/core/organizations/1/users/7/access-grant?accessTokenId=501");
    }
}
