package com.yasirkhan.tracking.exceptions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResourceAlreadyExistException extends RuntimeException{

    private String message;

    private HttpStatus status;

    public ResourceAlreadyExistException(String message){
        this.message = message;
        this.status = HttpStatus.CONFLICT;
    }

}
