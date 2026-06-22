package com.yasirkhan.tracking.services;

import com.yasirkhan.tracking.exceptions.ResourceNotFoundException;
import com.yasirkhan.tracking.models.UserPrincipal;
import com.yasirkhan.tracking.models.dtos.VehicleData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
public class TrackingService {

    private final RedisTemplate<String, Object> redisTemplate;

    public TrackingService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public List<VehicleData> getInitialPositions() {

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            throw new RuntimeException("Unauthorized: No valid session found.");
        }

        UserPrincipal principal = (UserPrincipal) auth.getPrincipal();
        String userId = principal.userId();
        String role = principal.role();

        if ("ADMIN".equals(role) || "ROLE_ADMIN".equals(role)) {
            return getAllLatestPositions();
        }
        else if ("SUPERVISOR".equals(role) || "ROLE_SUPERVISOR".equals(role)) {

            // Using your specific Redis key format
            String tehsilId = (String) redisTemplate.opsForHash().get("wtms:user:" + userId, "tehsilId");

            if (tehsilId == null || tehsilId.isEmpty()) {
                // Using the exact exception from your pattern
                throw new ResourceNotFoundException("No territory assigned to this supervisor.");
            }
            return getLatestPositionsByTehsil(tehsilId);

        } else {
            throw new RuntimeException("You do not have permission to view live tracking.");
        }
    }

    private List<VehicleData> getAllLatestPositions() {
        List<VehicleData> activePositions = new ArrayList<>();
        Set<String> vehicleKeys = redisTemplate.keys("wtms:live:vehicle:*");

        if (vehicleKeys != null) {
            for (String key : vehicleKeys) {
                Map<Object, Object> data = redisTemplate.opsForHash().entries(key);
                VehicleData dto = buildVehicleData(key, data);
                if (dto != null) activePositions.add(dto);
            }
        }
        log.debug("Found {} total active vehicles for Admin view", activePositions.size());
        return activePositions;
    }

    private List<VehicleData> getLatestPositionsByTehsil(String tehsilId) {
        List<VehicleData> activePositions = new ArrayList<>();

        // 1. Get the LIVE keys (because we only want trucks that are currently active)
        Set<String> liveVehicleKeys = redisTemplate.keys("wtms:live:vehicle:*");

        if (liveVehicleKeys != null) {
            for (String liveKey : liveVehicleKeys) {

                // 2. Extract the Registration Number (e.g., "LES-16-217")
                String vehicleNo = liveKey.replace("wtms:live:vehicle:", "");

                // 3. Look up the Tehsil ID in the ASSIGNMENT hash, not the live hash!
                String assignmentKey = "wtms:vehicle:" + vehicleNo;
                Object cachedTehsilIdObj = redisTemplate.opsForHash().get(assignmentKey, "tehsilId");
                String cachedTehsilId = cachedTehsilIdObj != null ? cachedTehsilIdObj.toString() : "";

                log.debug("Vehicle: {} -> Cached Tehsil Id: {}", vehicleNo, cachedTehsilId);

                // 4. If the Tehsil matches the Supervisor, grab the LIVE data and send it
                if (tehsilId.equals(cachedTehsilId)) {
                    Map<Object, Object> liveData = redisTemplate.opsForHash().entries(liveKey);
                    VehicleData dto = buildVehicleData(liveKey, liveData);
                    if (dto != null) activePositions.add(dto);
                }
            }
        }
        log.debug("Found {} active vehicles for Tehsil {}", activePositions.size(), tehsilId);
        return activePositions;
    }

    private VehicleData buildVehicleData(String key, Map<Object, Object> data) {
        Object latObj = data.get("latitude");
        Object lngObj = data.get("longitude");

        // We only build the DTO if we have valid coordinates
        if (latObj != null && lngObj != null) {
            VehicleData dto = new VehicleData();

            // Core Identifiers
            dto.setVehicleNo(key.replace("wtms:live:vehicle:", ""));

            Object deviceIdObj = data.get("deviceId");
            dto.setDeviceId(deviceIdObj != null ? deviceIdObj.toString() : "");

            // Location Data
            dto.setLatitude(Double.parseDouble(latObj.toString()));
            dto.setLongitude(Double.parseDouble(lngObj.toString()));

            Object locationObj = data.get("location");
            dto.setLocation(locationObj != null ? locationObj.toString() : "");

            // Telemetry Data
            Object speedObj = data.get("speed");
            dto.setSpeed(speedObj != null ? Double.parseDouble(speedObj.toString()) : 0.0);

            Object engineObj = data.get("engine");
            dto.setEngine(engineObj != null && Boolean.parseBoolean(engineObj.toString()));

            Object mileageObj = data.get("mileage");
            dto.setMileage(mileageObj != null ? Double.parseDouble(mileageObj.toString()) : 0.0);

            Object workingHoursObj = data.get("workingHours");
            dto.setWorkingHours(workingHoursObj != null ? Integer.parseInt(workingHoursObj.toString()) : 0);

            // Timestamps
            // Note: Checking "lastUpdated" as that is how your TrackingPollingJob saves it to Redis
            Object lastUpdateObj = data.get("lastUpdated");
            if (lastUpdateObj == null) {
                lastUpdateObj = data.get("lastUpdate"); // Fallback just in case
            }
            dto.setLastUpdate(lastUpdateObj != null ? lastUpdateObj.toString() : "");

            Object syncAtObj = data.get("mileageWorkingHoursLastSyncAt");
            dto.setMileageWorkingHoursLastSyncAt(syncAtObj != null ? syncAtObj.toString() : "");

            return dto;
        }
        return null;
    }
}