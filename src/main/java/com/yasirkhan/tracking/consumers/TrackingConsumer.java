package com.yasirkhan.tracking.consumers;

import com.yasirkhan.tracking.models.dtos.VehicleData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class TrackingConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisTemplate<String, Object> redisTemplate;

    // OPTIMIZATION: Local cache to prevent hammering Redis on high-frequency Kafka streams
    // Note: If you use a caching library like Caffeine, you can add a TTL (e.g., 5 mins) to this map automatically.
    private final Map<String, String> localTehsilCache = new ConcurrentHashMap<>();

    public TrackingConsumer(SimpMessagingTemplate messagingTemplate, RedisTemplate<String, Object> redisTemplate) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(
            topics = "live-coordinates-topic",
            groupId = "tracking-group",
            containerFactory = "listenerContainerFactory"
    )
    public void consumeVehicleLocation(VehicleData coordinateData) {
        log.debug("Processing location broadcast for vehicle: {}", coordinateData.getVehicleNo());

        // Send to Admins (Global track channel)
        messagingTemplate.convertAndSend("/topic/tracking/all", coordinateData);

        // Fetch assigned territory from local cache first, fallback to Redis
        String tehsilId = localTehsilCache.computeIfAbsent(coordinateData.getVehicleNo(), this::fetchTehsilIdFromRedis);

        // Send to specific Supervisor Tehsil channel
        if (tehsilId != null && !tehsilId.isEmpty()) {
            String supervisorTopic = "/topic/tracking/tehsil/" + tehsilId;
            messagingTemplate.convertAndSend(supervisorTopic, coordinateData);
        }
    }

    private String fetchTehsilIdFromRedis(String vehicleNo) {
        String scheduleKey = "wtms:vehicle:" + vehicleNo;
        String tehsilId = (String) redisTemplate.opsForHash().get(scheduleKey, "tehsilId");
        return tehsilId != null ? tehsilId : "";
    }

    // Optional: Call this method if you ever re-assign a vehicle to a new Tehsil mid-shift
    public void clearVehicleFromCache(String vehicleNo) {
        localTehsilCache.remove(vehicleNo);
    }
}