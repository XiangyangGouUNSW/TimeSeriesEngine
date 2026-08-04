package com.sfkg.timeseries.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public final class JsonSuccessResponse {

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

    private JsonSuccessResponse() {
    }

    public static ApiResult<Void> parseAndReturn(String requestBody, String successMessage) throws JsonProcessingException {
        parseJson(requestBody);
        return returnSuccess(successMessage);
    }

    public static ApiResult<Void> returnSuccess(String successMessage) {
        System.out.println(successMessage);
        return ApiResult.successMessage(successMessage);
    }

    public static <T> ApiResult<T> returnSuccess(String successMessage, T data) {
        System.out.println(successMessage);
        return ApiResult.success(successMessage, data);
    }

    private static void parseJson(String requestBody) throws JsonProcessingException {
        String json = requestBody == null ? "" : requestBody.trim();
        if (json.isEmpty()) {
            json = "{}";
        }
        OBJECT_MAPPER.readTree(json);
    }
}
