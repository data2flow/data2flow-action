package net.java21.data2flow.action.notification.controller;

import jakarta.servlet.http.HttpServletRequest;
import net.java21.data2flow.action.notification.NotificationErrorCode;
import net.java21.data2flow.action.notification.repository.DeliveryRepository.DeliveryQuery;
import net.java21.data2flow.action.notification.service.ChannelAdminService;
import net.java21.data2flow.action.notification.service.ChannelAdminService.ChannelType;
import net.java21.data2flow.action.notification.service.ChannelAdminService.DeliveryView;
import net.java21.data2flow.action.notification.service.ChannelAdminService.SystemNotification;
import net.java21.data2flow.action.notification.service.ChannelAdminService.TestRequest;
import net.java21.data2flow.action.notification.service.ChannelAdminService.TestResult;
import net.java21.data2flow.action.notification.service.MessengerCallbackService;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.identity.CurrentUser;
import net.java21.data2flow.contracts.identity.CurrentUserHolder;
import net.java21.data2flow.contracts.notification.InboundRequest;
import net.java21.data2flow.contracts.notification.LinkResult;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.contracts.web.CursorListApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * 알림 내부 API(ADR-021: ClusterIP·토큰 없음). core-api가 외부 API(API-RUL-27·30, API-OPS-31·33·34·97)의 권한을 본 뒤 신원 헤더와 함께
 * 넘기고, BFF가 메신저 콜백(API-RUL-31)을 원본 그대로 넘긴다. 콜백은 신원 헤더가 없고 채널 시크릿으로 검증한다.
 */
@RestController
public class InternalNotificationController {

    private final ChannelAdminService admin;
    private final MessengerCallbackService callbacks;

    public InternalNotificationController(ChannelAdminService admin, MessengerCallbackService callbacks) {
        this.admin = admin;
        this.callbacks = callbacks;
    }

    /** API-RUL-31 메신저 콜백(BFF → 원본 그대로). 시크릿이 틀리면 401 */
    @PostMapping("/internal/action/notifications/callbacks/{channel}")
    public ApiResponse<MessengerCallbackService.Result> callback(@PathVariable String channel, @RequestBody(required = false) byte[] body,
                                                                HttpServletRequest request) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            headers.put(name, request.getHeader(name));
        }
        return ApiResponse.success(callbacks.handle(channel, new InboundRequest(headers, body)));
    }

    /** API-OPS-97 시스템 알림 직접 발송(core 시스템 알림, ai 리포트) → 202 */
    @PostMapping("/internal/action/notifications")
    public ResponseEntity<ApiResponse<Map<String, String>>> notify(@RequestBody SystemNotification body) {
        String id = admin.systemNotify(user().organizationId(), body);
        return ResponseEntity.accepted().body(ApiResponse.success(Map.of("notificationId", id)));
    }

    /** API-RUL-27 발송 이력(커서 목록, 최신순) */
    @GetMapping("/internal/action/notifications/deliveries")
    public CursorListApiResponse<DeliveryView> deliveries(@RequestParam(required = false) Long alarmId,
                                                          @RequestParam(required = false) Long channelId,
                                                          @RequestParam(required = false) String status,
                                                          @RequestParam(required = false) Instant from,
                                                          @RequestParam(required = false) Instant to,
                                                          @RequestParam(required = false) String cursor,
                                                          @RequestParam(required = false) Integer size) {
        return admin.list(new DeliveryQuery(user().organizationId(), alarmId, channelId,
                status == null || status.isBlank() ? null : status.toUpperCase(Locale.ROOT), from, to), cursor, size);
    }

    /** API-OPS-33 다시 보내기(성공 건은 409) */
    @PostMapping("/internal/action/notifications/deliveries/{delivery-id}/resend")
    public ApiResponse<Map<String, String>> resend(@PathVariable("delivery-id") String deliveryId) {
        UUID id;
        try {
            id = UUID.fromString(deliveryId);
        } catch (IllegalArgumentException e) {
            throw new BusinessException(NotificationErrorCode.DELIVERY_NOT_FOUND);
        }
        return ApiResponse.success(Map.of("newDeliveryId", admin.resend(user().organizationId(), id)));
    }

    /** API-OPS-31 테스트 발송. 실패면 502 CHANNEL_TEST_FAILED */
    @PostMapping("/internal/action/notifications/channels/test")
    public ApiResponse<TestResult> test(@RequestBody TestRequest body) {
        return ApiResponse.success(admin.test(user().organizationId(), body));
    }

    /** OPS-06.01 저장 때 콜백 주소 등록(텔레그램 setWebhook) */
    @PostMapping("/internal/action/notifications/channels/{channel-id}/webhook")
    public ApiResponse<Map<String, Object>> webhook(@PathVariable("channel-id") long channelId) {
        return ApiResponse.success(admin.registerWebhook(user().organizationId(), channelId));
    }

    /** API-OPS-34 채널 유형 */
    @GetMapping("/internal/action/notifications/channel-types")
    public ApiResponse<List<ChannelType>> types() {
        return ApiResponse.success(admin.types());
    }

    /** API-RUL-30 계정 연결 딥링크 요청 */
    public record LinkBody(String channel, String organizationId, String userId, String code, Instant expiresAt) {
    }

    @PostMapping("/internal/action/notifications/links")
    public ApiResponse<LinkResult> link(@RequestBody LinkBody body) {
        if (body.channel() == null || body.code() == null || body.expiresAt() == null) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("channel·code·expiresAt", "REQUIRED", "필수입니다")));
        }
        CurrentUser u = CurrentUserHolder.find().orElse(null);
        long org = body.organizationId() != null ? Long.parseLong(body.organizationId()) : u == null ? 0 : u.organizationId();
        long userId = body.userId() != null ? Long.parseLong(body.userId()) : u == null ? 0 : u.userId();
        if (org < 1 || userId < 1) {
            throw new BusinessException(CommonErrorCode.AUTH_TOKEN_INVALID);
        }
        return ApiResponse.success(admin.link(org, body.channel(), userId, body.code(), body.expiresAt()));
    }

    /** core가 넘긴 신원(X-USER-ID·X-ORG-ID). 없으면 401 */
    private static CurrentUser user() {
        return CurrentUserHolder.find().orElseThrow(() -> new BusinessException(CommonErrorCode.AUTH_TOKEN_INVALID));
    }
}
