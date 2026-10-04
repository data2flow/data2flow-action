package net.java21.data2flow.action.notification.repository;

import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** 묶음 발송 창(notification_aggregates): 정책 묶기 창(BR-RUL-15), 채널 기본 묶음·한도(BR-OPS-07), 방해 금지 끝 요약(OPS-06.05) */
@Repository
public class AggregateRepository {

    private final JdbcClient jdbc;

    public AggregateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 열린 창을 잠가 가져오고, 없으면 만든다(동시에 만들면 UNIQUE로 하나만 남고 다시 읽는다) */
    public Aggregate lockOrOpen(long organizationId, String channelType, long channelId, String recipientKey, String aggregateKey, String kind,
                                Instant start, Instant end, String env) {
        Optional<Aggregate> open = lockOpen(organizationId, channelId, recipientKey, aggregateKey, kind);
        if (open.isPresent()) {
            return open.get();
        }
        jdbc.sql("""
                        INSERT INTO data2flow_action.notification_aggregates (organization_id, channel_type, channel_id, recipient_key, aggregate_key,
                            kind, window_start, window_end, env, created_at)
                        VALUES (:org, :ct, :ch, :rk, :ak, :kind, :start, :end, :env, :start)
                        ON CONFLICT (organization_id, channel_id, recipient_key, aggregate_key, kind) WHERE status = 'OPEN' DO NOTHING""")
                .param("org", organizationId).param("ct", channelType).param("ch", channelId).param("rk", cut(recipientKey, 160))
                .param("ak", cut(aggregateKey, 200)).param("kind", kind).param("start", Pg.ts(start)).param("end", Pg.ts(end))
                .param("env", env).update();
        return lockOpen(organizationId, channelId, recipientKey, aggregateKey, kind).orElseThrow();
    }

    private Optional<Aggregate> lockOpen(long organizationId, long channelId, String recipientKey, String aggregateKey, String kind) {
        return jdbc.sql("""
                        SELECT * FROM data2flow_action.notification_aggregates
                         WHERE organization_id = :org AND channel_id = :ch AND recipient_key = :rk AND aggregate_key = :ak AND kind = :kind
                           AND status = 'OPEN' FOR UPDATE""")
                .param("org", organizationId).param("ch", channelId).param("rk", cut(recipientKey, 160)).param("ak", cut(aggregateKey, 200))
                .param("kind", kind).query(AggregateRepository::map).optional();
    }

    public void count(long organizationId, long id, boolean sentNow) {
        jdbc.sql("""
                        UPDATE data2flow_action.notification_aggregates
                           SET item_count = item_count + 1, sent_count = sent_count + CASE WHEN :sent THEN 1 ELSE 0 END
                         WHERE id = :id AND organization_id = :org""")
                .param("sent", sentNow).param("id", id).param("org", organizationId).update();
    }

    @OrganizationScopeExempt("묶음 작업은 이 배포의 모든 조직 창을 처리한다")
    public List<Aggregate> lockDue(Instant now, int limit, String env) {
        return jdbc.sql("""
                        SELECT * FROM data2flow_action.notification_aggregates
                         WHERE status = 'OPEN' AND window_end <= :now AND env = :env
                         ORDER BY window_end LIMIT :limit FOR UPDATE SKIP LOCKED""")
                .param("now", Pg.ts(now)).param("env", env).param("limit", limit).query(AggregateRepository::map).list();
    }

    public void flush(long organizationId, long id, UUID summaryDeliveryId) {
        jdbc.sql("""
                        UPDATE data2flow_action.notification_aggregates SET status = 'FLUSHED', summary_delivery_id = :s
                         WHERE id = :id AND organization_id = :org""")
                .param("s", summaryDeliveryId).param("id", id).param("org", organizationId).update();
    }

    /**
     * 묶음 창.
     *
     * @param kind EXPLICIT·CHANNEL·DND
     */
    public record Aggregate(long id, long organizationId, String channelType, long channelId, String recipientKey, String aggregateKey,
                            String kind, Instant windowStart, Instant windowEnd, int itemCount, int sentCount) {
    }

    static Aggregate map(ResultSet rs, int n) throws SQLException {
        return new Aggregate(rs.getLong("id"), rs.getLong("organization_id"), rs.getString("channel_type"), rs.getLong("channel_id"),
                rs.getString("recipient_key"), rs.getString("aggregate_key"), rs.getString("kind"), Pg.instant(rs, "window_start"),
                Pg.instant(rs, "window_end"), rs.getInt("item_count"), rs.getInt("sent_count"));
    }

    private static String cut(String s, int max) {
        return s == null ? null : (s.length() <= max ? s : s.substring(0, max));
    }
}
