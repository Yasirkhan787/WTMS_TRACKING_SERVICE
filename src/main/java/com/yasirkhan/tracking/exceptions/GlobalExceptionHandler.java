package com.yasirkhan.tracking.exceptions;

import com.yasirkhan.tracking.responses.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(DataBaseException.class)
    public ResponseEntity<ErrorResponse> handleDataBaseException(DataBaseException ex, HttpServletRequest request) {

        log.error("Database Exception at {}: {}", request.getRequestURI(), ex.getMessage(), ex);

        ErrorResponse response =
                ErrorResponse
                        .builder()
                        .message(ex.getMessage())
                        .status(ex.getStatus().value())
                        .error(ex.getStatus().getReasonPhrase())
                        .path(request.getRequestURI())
                        .timeStamp(LocalDateTime.now())
                        .build();
        return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({UnauthorizedException.class, io.jsonwebtoken.JwtException.class})
    public ResponseEntity<ErrorResponse> handleAuthenticationExceptions(Exception exception, HttpServletRequest request) {

        log.error("Authentication/JWT Error at {}: {}", request.getRequestURI(), exception.getMessage(), exception);

        // If it's a raw JwtException (like signature tampered), customize the message
        String message = (exception instanceof io.jsonwebtoken.JwtException)
                ? "Invalid or Expired JWT Token"
                : exception.getMessage();

        ErrorResponse response =
                ErrorResponse
                        .builder()
                        .message(message)
                        .status(HttpStatus.UNAUTHORIZED.value()) // 401
                        .error(HttpStatus.UNAUTHORIZED.getReasonPhrase())
                        .timeStamp(LocalDateTime.now())
                        .path(request.getRequestURI())
                        .build();

        return new ResponseEntity<>(response, HttpStatus.UNAUTHORIZED);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleResourceNotFoundException(ResourceNotFoundException ex, HttpServletRequest request) {

        log.warn("Resource Not Found at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse response =
                ErrorResponse
                        .builder()
                        .message(ex.getMessage())
                        .status(ex.getStatus().value())
                        .error(ex.getStatus().getReasonPhrase())
                        .path(request.getRequestURI())
                        .timeStamp(LocalDateTime.now())
                        .build();
        return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(ResourceAlreadyExistException.class)
    public ResponseEntity<ErrorResponse> handleResourceNotFoundException(ResourceAlreadyExistException ex, HttpServletRequest request) {

        log.warn("Resource Already Found at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse response =
                ErrorResponse
                        .builder()
                        .message(ex.getMessage())
                        .status(ex.getStatus().value())
                        .error(ex.getStatus().getReasonPhrase())
                        .path(request.getRequestURI())
                        .timeStamp(LocalDateTime.now())
                        .build();
        return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(ClassCastException.class)
    public ResponseEntity<ErrorResponse> handleClassCastException(ClassCastException ex, HttpServletRequest request) {

        log.error("Data Type Mismatch / Casting Error at {}: {}", request.getRequestURI(), ex.getMessage(), ex);

        ErrorResponse response =
                ErrorResponse
                        .builder()
                        .message("Data type mismatch in request payload. Details: " + ex.getMessage())
                        .status(HttpStatus.BAD_REQUEST.value())
                        .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
                        .path(request.getRequestURI())
                        .timeStamp(LocalDateTime.now())
                        .build();

        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ErrorResponse> handleGlobalRuntimeException(RuntimeException ex, HttpServletRequest request) {

        log.error("Unexpected Runtime Exception caught at {}: {}", request.getRequestURI(), ex.getMessage(), ex);

        ErrorResponse response =
                ErrorResponse
                        .builder()
                        .message("An unexpected internal server error occurred.")
                        .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .error(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase())
                        .path(request.getRequestURI())
                        .timeStamp(LocalDateTime.now())
                        .build();

        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}