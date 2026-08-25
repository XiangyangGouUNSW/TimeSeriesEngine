package com.sfkg.timeseries.common;

import java.security.SecureRandom;

/**
 * 语义化 ID 生成器：将业务语义片段（如 categoryId、deviceName、relationType 等）
 * 清洗拼接后追加随机后缀，保证 ID 可读且全局唯一。
 *
 * <p>格式：{@code <part1>_<part2>_..._<random8>}；非 ASCII 片段会被过滤，
 * 全部为空时回退为 {@code entity_<random8>}。</p>
 */
public final class SemanticId {

    private static final String SUFFIX_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int SUFFIX_LENGTH = 8;
    private static final int MAX_PART_LENGTH = 24;
    private static final int MAX_BASE_LENGTH = 80;
    private static final String FALLBACK = "entity";
    private static final SecureRandom RANDOM = new SecureRandom();

    private SemanticId() {
    }

    /**
     * 生成语义化 ID。每个入参为一个语义片段，null/空白/无 ASCII 字符的片段会被跳过。
     */
    public static String generate(String... semanticParts) {
        StringBuilder base = new StringBuilder();
        for (String part : semanticParts) {
            String cleaned = sanitize(part);
            if (!cleaned.isEmpty()) {
                if (base.length() > 0) {
                    base.append('_');
                }
                base.append(cleaned);
            }
        }
        if (base.length() == 0) {
            base.append(FALLBACK);
        }
        if (base.length() > MAX_BASE_LENGTH) {
            base.setLength(MAX_BASE_LENGTH);
            while (base.length() > 0 && base.charAt(base.length() - 1) == '_') {
                base.setLength(base.length() - 1);
            }
        }
        return base.append('_').append(randomSuffix()).toString();
    }

    private static String sanitize(String value) {
        if (value == null) {
            return "";
        }
        String cleaned = value.trim().toLowerCase()
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
        return cleaned.length() > MAX_PART_LENGTH ? cleaned.substring(0, MAX_PART_LENGTH) : cleaned;
    }

    private static String randomSuffix() {
        StringBuilder suffix = new StringBuilder(SUFFIX_LENGTH);
        for (int i = 0; i < SUFFIX_LENGTH; i++) {
            suffix.append(SUFFIX_ALPHABET.charAt(RANDOM.nextInt(SUFFIX_ALPHABET.length())));
        }
        return suffix.toString();
    }
}
