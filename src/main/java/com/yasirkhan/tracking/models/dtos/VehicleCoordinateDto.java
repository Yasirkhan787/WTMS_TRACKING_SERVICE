package com.yasirkhan.tracking.models.dtos;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehicleCoordinateDto implements Serializable {

    private String status;
    @JsonProperty("data")
    private List<VehicleData> vehicleData;
}