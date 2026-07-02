package com.yasirkhan.tracking.consumers;

import com.yasirkhan.tracking.models.dtos.VehicleData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TrackingConsumerMockTopic {

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisTemplate<String, Object> redisTemplate;

    public TrackingConsumerMockTopic(SimpMessagingTemplate messagingTemplate, RedisTemplate<String, Object> redisTemplate) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(
            topics = "live-coordinates-mock-topic",
            groupId = "tracking-group",
            containerFactory = "listenerContainerFactory"
    )
    public void consumeVehicleLocation(VehicleData coordinateData) {
        log.info("Processing location broadcast for vehicle: {}", coordinateData.getVehicleNo());

        // Send to Admins (Global track channel)
        messagingTemplate.convertAndSend("/topic/tracking/all", coordinateData);

        // Fetch assigned territory from Redis to filter for Supervisors
        String key = "wtms:vehicle:" + coordinateData.getVehicleNo();
        String tehsilId = (String) redisTemplate.opsForHash().get(key, "tehsilId");

        // 3. Send to specific Supervisor Tehsil channel
        if (tehsilId != null && !tehsilId.isEmpty()) {
            String supervisorTopic = "/topic/tracking/tehsil/" + tehsilId;
            messagingTemplate.convertAndSend(supervisorTopic, coordinateData);
        }
    }
}