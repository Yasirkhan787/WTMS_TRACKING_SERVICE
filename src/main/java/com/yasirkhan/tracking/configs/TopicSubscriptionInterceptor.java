package com.yasirkhan.tracking.configs;

import com.yasirkhan.tracking.models.UserPrincipal;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Component;

import java.security.Principal;

@Component
public class TopicSubscriptionInterceptor implements ChannelInterceptor {

    private final RedisTemplate<String, Object> redisTemplate;

    public TopicSubscriptionInterceptor(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);

        if (accessor != null) {

            // CLEANUP: If the client unsubscribes, clear memory
            if (StompCommand.UNSUBSCRIBE.equals(accessor.getCommand()) && accessor.getSubscriptionId() != null) {
                WebSocketTrafficInterceptor.autoSubscriptions.remove(accessor.getSubscriptionId());
                return message;
            }

            if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
                String destination = accessor.getDestination();

                if ("/topic/tracking/auto".equals(destination)) {
                    Principal principal = accessor.getUser();

                    if (principal instanceof UsernamePasswordAuthenticationToken auth) {
                        UserPrincipal userPrincipal = (UserPrincipal) auth.getPrincipal();
                        String role = userPrincipal.role();
                        String userId = userPrincipal.userId();

                        String routedDestination;

                        if (role.equalsIgnoreCase("ADMIN") || role.equalsIgnoreCase("ROLE_ADMIN")) {
                            routedDestination = "/topic/tracking/all";
                        }
                        else if (role.equalsIgnoreCase("SUPERVISOR") || role.equalsIgnoreCase("ROLE_SUPERVISOR")) {
                            String userKey = "wtms:user:" + userId;
                            String tehsilId = (String) redisTemplate.opsForHash().get(userKey, "tehsilId");

                            if (tehsilId == null || tehsilId.isEmpty()) {
                                throw new IllegalArgumentException("Supervisor has no assigned Tehsil ID in Redis");
                            }
                            routedDestination = "/topic/tracking/tehsil/" + tehsilId;
                        }
                        else {
                            throw new IllegalArgumentException("User role not authorized for live tracking");
                        }

                        // ---> THE FIX: Save the Subscription ID instead of Session ID <---
                        String subId = accessor.getSubscriptionId();
                        if (subId != null) {
                            WebSocketTrafficInterceptor.autoSubscriptions.put(subId, true);
                        }
                        // -----------------------------------------------------------------

                        StompHeaderAccessor newAccessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
                        newAccessor.setSessionId(accessor.getSessionId());
                        newAccessor.setSessionAttributes(accessor.getSessionAttributes());
                        newAccessor.setUser(accessor.getUser());
                        newAccessor.setSubscriptionId(subId);

                        if (accessor.getNativeHeader("id") != null) {
                            newAccessor.setNativeHeader("id", accessor.getNativeHeader("id").get(0));
                        }

                        newAccessor.setDestination(routedDestination);
                        newAccessor.setNativeHeader("destination", routedDestination);

                        return MessageBuilder.createMessage(message.getPayload(), newAccessor.getMessageHeaders());
                    }
                }
            }
        }

        return message;
    }
}