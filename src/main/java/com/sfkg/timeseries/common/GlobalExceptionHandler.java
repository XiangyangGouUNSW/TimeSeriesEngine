package com.sfkg.timeseries.common;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BusinessException.class)
    public ApiResult<Void> handleBusinessException(BusinessException exception) {
        return ApiResult.fail(exception.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ApiResult<Void> handleSystemException(Exception exception) {
        return ApiResult.fail("system error");
    }
}
