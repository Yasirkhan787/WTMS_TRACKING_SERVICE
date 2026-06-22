package com.yasirkhan.tracking.configs;

import com.yasirkhan.tracking.models.UserPrincipal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import java.security.Principal;

@Component
@Slf4j
public class WebSocketEventListener {

    /**
     * 1. Triggered when a new STOMP CONNECT request comes in.
     * This represents a user TRYING to connect.
     */
    @EventListener
    public void handleWebSocketConnectListener(SessionConnectEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        UserPrincipal user = getUserPrincipal(accessor.getUser());

        if (user != null) {
            log.info("SOCKET CONNECT ATTEMPT: User '{}' (ID: {}, Role: {}) attempting to connect with Session ID: {}",
                    user.username(), user.userId(), user.role(), accessor.getSessionId());
        } else {
            log.warn("SOCKET CONNECT ATTEMPT: Unauthenticated or unknown user attempting to connect. Session ID: {}",
                    accessor.getSessionId());
        }
    }

    /**
     * 2. Triggered when the broker has processed the CONNECT and replied with CONNECTED.
     * This represents a SUCCESSFUL connection.
     */
    @EventListener
    public void handleWebSocketConnectedListener(SessionConnectedEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        UserPrincipal user = getUserPrincipal(accessor.getUser());

        if (user != null) {
            log.info("SOCKET CONNECTED: User '{}' is now live on Session ID: {}",
                    user.username(), accessor.getSessionId());
        }
    }

    /**
     * 3. Triggered when a STOMP session ends (either user closes browser, network drops, or explicit disconnect).
     */
    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        UserPrincipal user = getUserPrincipal(accessor.getUser());

        if (user != null) {
            log.info("SOCKET DISCONNECTED: User '{}' has disconnected from Session ID: {}",
                    user.username(), accessor.getSessionId());
        } else {
            log.info("SOCKET DISCONNECTED: Unauthenticated Session ID: {} closed.",
                    accessor.getSessionId());
        }
    }

    /**
     * 4. Triggered when a client explicitly SUBSCRIBES to a topic.
     * This proves whether the frontend is actually asking for the data.
     */
    @EventListener
    public void handleWebSocketSubscribeListener(SessionSubscribeEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        UserPrincipal user = getUserPrincipal(accessor.getUser());
        String destination = accessor.getDestination();

        if (user != null) {
            log.info("SOCKET SUBSCRIBE: User '{}' (Session: {}) successfully subscribed to topic: [{}]",
                    user.username(), accessor.getSessionId(), destination);
        } else {
            log.info("SOCKET SUBSCRIBE: Unauthenticated Session: {} subscribed to topic: [{}]",
                    accessor.getSessionId(), destination);
        }
    }

    /**
     * Helper method to cleanly extract your custom UserPrincipal
     * using the exact same logic you used in your Interceptor.
     */
    private UserPrincipal getUserPrincipal(Principal principal) {
        if (principal instanceof UsernamePasswordAuthenticationToken auth) {
            if (auth.getPrincipal() instanceof UserPrincipal userPrincipal) {
                return userPrincipal;
            }
        }
        return null;
    }
}