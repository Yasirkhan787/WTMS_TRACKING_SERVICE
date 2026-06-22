package com.yasirkhan.tracking.configs;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final TopicSubscriptionInterceptor subscriptionInterceptor;
    private final WebSocketTrafficInterceptor trafficInterceptor;

    public WebSocketConfig(TopicSubscriptionInterceptor subscriptionInterceptor,
                           WebSocketTrafficInterceptor trafficInterceptor) {
        this.subscriptionInterceptor = subscriptionInterceptor;
        this.trafficInterceptor = trafficInterceptor;
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/tracking")
                // OPTIMIZATION: Replace '*' with your actual frontend/mobile gateway domains
                .setAllowedOriginPatterns("*"); //("https://your-frontend-domain.com", "https://*.your-internal-network.com");
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(subscriptionInterceptor);
    }

    @Override
    public void configureClientOutboundChannel(ChannelRegistration registration) {
        registration.interceptors(trafficInterceptor);
    }
}