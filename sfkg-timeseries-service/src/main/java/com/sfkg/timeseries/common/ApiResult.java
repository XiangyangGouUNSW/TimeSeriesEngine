package com.sfkg.timeseries.common;

import lombok.Data;

@Data
public class ApiResult<T> {

    private boolean success;
    private String message;
    private T data;

    public static ApiResult<Void> success() {
        return successMessage("success");
    }

    public static <T> ApiResult<T> success(T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setSuccess(true);
        result.setMessage("success");
        result.setData(data);
        return result;
    }

    public static ApiResult<Void> successMessage(String message) {
        ApiResult<Void> result = new ApiResult<>();
        result.setSuccess(true);
        result.setMessage(message);
        return result;
    }

    public static <T> ApiResult<T> success(String message, T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setSuccess(true);
        result.setMessage(message);
        result.setData(data);
        return result;
    }

    public static <T> ApiResult<T> fail(String message) {
        ApiResult<T> result = new ApiResult<>();
        result.setSuccess(false);
        result.setMessage(message);
        return result;
    }
}
