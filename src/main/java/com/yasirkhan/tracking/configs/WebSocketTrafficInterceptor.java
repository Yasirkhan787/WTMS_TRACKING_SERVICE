package com.yasirkhan.tracking.configs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class WebSocketTrafficInterceptor implements ChannelInterceptor {

    public static final Map<String, Boolean> autoSubscriptions = new ConcurrentHashMap<>();

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);

        // THE FIX: Internal Spring messages don't have STOMP commands yet!
        // We must check the internal SimpMessageType instead.
        if (SimpMessageType.MESSAGE.equals(accessor.getMessageType())) {

            String originalDestination = accessor.getDestination();
            String subId = accessor.getSubscriptionId();

            // Check if this specific Subscription ID is in our memory map
            if (subId != null && autoSubscriptions.containsKey(subId)) {

                // Create a mutable copy of the headers
                StompHeaderAccessor newAccessor = StompHeaderAccessor.create(StompCommand.MESSAGE);
                newAccessor.copyHeaders(message.getHeaders());
                newAccessor.setLeaveMutable(true);

                // OVERWRITE THE DESTINATION FOR THE ANDROID APP
                newAccessor.setDestination("/topic/tracking/auto");
                newAccessor.setNativeHeader("destination", "/topic/tracking/auto");

                log.info("✨ OUTBOUND TRICK SUCCESS: Rewrote [{}] to [/topic/tracking/auto] for SubID: {}", originalDestination, subId);

                return MessageBuilder.createMessage(message.getPayload(), newAccessor.getMessageHeaders());
            }
        }

        return message;
    }
}