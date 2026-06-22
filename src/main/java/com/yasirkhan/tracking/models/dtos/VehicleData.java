package com.yasirkhan.tracking.models.dtos;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehicleData {

    private String deviceId;

    @JsonProperty("registrationNumber")
    private String vehicleNo;
    private double latitude;
    private double longitude;
    private double speed;
    private boolean engine;
    private String lastUpdate;
    private String location;
    private double mileage;
    private int workingHours;
    private String mileageWorkingHoursLastSyncAt;
}