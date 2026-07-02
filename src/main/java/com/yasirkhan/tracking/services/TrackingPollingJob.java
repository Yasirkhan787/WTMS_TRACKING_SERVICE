package com.yasirkhan.tracking.services;

import com.yasirkhan.tracking.models.dtos.VehicleCoordinateDto;
import com.yasirkhan.tracking.models.dtos.VehicleData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.*;

@Service
@Slf4j
public class TrackingPollingJob {

    private final RedisTemplate<String, Object> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RestClient restClient;

    @Value("${tracking.api.url}")
    private String thirdPartyApiUrl;

    @Value("${tracking.api.key}")
    private String apiKey;

    @Value("${tracking.api.fallback-url}")
    private String customFallbackUrl;

    @Value("${tracking.kafka.topic:live-coordinates-topic}")
    private String kafkaTopic;

    @Value("${tracking.kafka.mock-topic:live-coordinates-mock-topic}")
    private String mockTopic;

    public TrackingPollingJob(RedisTemplate<String, Object> redisTemplate,
                              KafkaTemplate<String, Object> kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.restClient = RestClient.builder().build();
    }

    @Scheduled(fixedRate = 60000)
    public void pollThirdPartyTrackingApi() {
        log.info("Starting scheduled 3rd-party tracking API ingestion cycle...");

        Set<String> activeVehicleKeys = redisTemplate.execute((RedisCallback<Set<String>>) connection -> {
            Set<String> keys = new HashSet<>();
            Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().match("wtms:vehicle:*").count(100).build());
            while (cursor.hasNext()) {
                keys.add(new String(cursor.next()));
            }
            return keys;
        });

        if (activeVehicleKeys == null || activeVehicleKeys.isEmpty()) {
            log.info("No active vehicles found in cache schedules for tracking. Skipping cycle.");
            return;
        }

        List<String> vehicleRegNumbers = new ArrayList<>();
        for (String key : activeVehicleKeys) {
            vehicleRegNumbers.add(key.replace("wtms:vehicle:", ""));
        }

        VehicleCoordinateDto responseData = null;
        boolean isFromFallback = false;

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("registrationNumbers", vehicleRegNumbers);
        requestBody.put("wmc", "RWMC");

        try {

            try {
                responseData = restClient.post()
                        .uri(thirdPartyApiUrl)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("x-api-key", apiKey)
                        .body(requestBody)
                        .retrieve()
                        .body(VehicleCoordinateDto.class);

                if (isInvalidResponse(responseData)) {
                    log.warn("Primary API returned a logical error or empty data. Triggering fallback.");
                    responseData = executeFallbackCall(requestBody);
                    isFromFallback = true;
                }
            } catch (RestClientException e) {
                log.error("Network error reaching primary AskTrack API: {}. Triggering fallback.", e.getMessage());
                responseData = executeFallbackCall(requestBody);
                isFromFallback = true;
            }

            if (isInvalidResponse(responseData)) {
                log.error("Both Primary and Fallback APIs failed to return valid coordinate data. Cycle aborted.");
                return;
            }

            String targetKafkaTopic = isFromFallback ? mockTopic : kafkaTopic;
            log.info("Routing {} vehicle updates to Kafka topic: {}", responseData.getVehicleData().size(), targetKafkaTopic);

            for (VehicleData liveData : responseData.getVehicleData()) {
                String hashKey = "wtms:live:vehicle:" + liveData.getVehicleNo();
                Map<String, String> updates = new HashMap<>();

                updates.put("deviceId", liveData.getDeviceId());
                updates.put("latitude", String.valueOf(liveData.getLatitude()));
                updates.put("longitude", String.valueOf(liveData.getLongitude()));
                updates.put("engine", String.valueOf(liveData.isEngine()));
                updates.put("speed", String.valueOf(liveData.getSpeed()));
                updates.put("lastUpdated", liveData.getLastUpdate());
                updates.put("location", liveData.getLocation());
                updates.put("mileage", String.valueOf(liveData.getMileage()));
                updates.put("workingHours", String.valueOf(liveData.getWorkingHours()));
                updates.put("mileageWorkingHoursLastSyncAt", String.valueOf(liveData.getMileageWorkingHoursLastSyncAt()));

                redisTemplate.opsForHash().putAll(hashKey, updates);
                kafkaTemplate.send(targetKafkaTopic, liveData.getVehicleNo(), liveData);
            }

        } catch (Exception e) {
            log.error("Unexpected error during tracking ingestion cycle execution: {}", e.getMessage(), e);
        }
    }

    private VehicleCoordinateDto executeFallbackCall(Map<String, Object> requestBody) {
        try {
            log.info("Executing fallback API call to: {}", customFallbackUrl);
            return restClient.post()
                    .uri(customFallbackUrl)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(requestBody)
                    .retrieve()
                    .body(VehicleCoordinateDto.class);
        } catch (Exception e) {
            log.error("Fallback API also failed: {}", e.getMessage());
            return null;
        }
    }

    private boolean isInvalidResponse(VehicleCoordinateDto responseData) {
        return responseData == null ||
                "error".equalsIgnoreCase(responseData.getStatus()) ||
                responseData.getVehicleData() == null ||
                responseData.getVehicleData().isEmpty();
    }
}