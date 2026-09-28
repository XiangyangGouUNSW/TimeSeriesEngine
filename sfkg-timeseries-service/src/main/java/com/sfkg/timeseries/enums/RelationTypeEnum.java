package com.sfkg.timeseries.enums;

/**
 * 语义关系类型：S / C / P 三端共用规范取值（2026-08-13 定）。
 *
 * <p>大小写不敏感：S 端存储统一归一化为大写；下发 C 端时统一转小写，因为
 * C 端 {@code statistics_service.cpp} 用 {@code relation.relation_type != "correlation"}
 * 做精确匹配（不匹配就直接 FAILED_PRECONDITION），所以大小写转换是必需行为，
 * 见 {@code TimeseriesCoreGrpcClient#syncRelation}。
 *
 * <p>规范前的自由字符串（含 {@code MUTUAL} / {@code COUPLING} / {@code BIDIRECTIONAL} /
 * {@code COUPLED} 关键词）不得作为新值写入：P 端保留这段兼容逻辑，会把含这些子串的
 * relation_type 当成"互耦标记"，语义与规范值冲突。
 *
 * <p>P 端语义（{@code sfkg-timeseries-analysis/src/analysis_engine.py}）：
 * <ul>
 *   <li>{@link #CAUSE} / {@link #CAUSAL} —— 因果型：进 GCAD 因果候选结构门，
 *       并参与互耦双向识别（A→B 且 B→A 成对即互耦）</li>
 *   <li>{@link #CORRELATION} / {@link #ASSOCIATION} —— 无向相关/关联：不算因果候选，
 *       也不成互耦对</li>
 * </ul>
 */
public enum RelationTypeEnum {

    /** 因果关系：源影响目标，有方向。 */
    CAUSE(true),

    /** 因果关系：与 {@link #CAUSE} 同义，规范保留两种写法。 */
    CAUSAL(true),

    /** 相关性：无向。 */
    CORRELATION(false),

    /** 关联 / 伴随：无向。 */
    ASSOCIATION(false);

    private final boolean causal;

    RelationTypeEnum(boolean causal) {
        this.causal = causal;
    }

    /**
     * 是否为因果型。P 端的 GCAD 因果候选门（{@code _extract_relations_prior}）与
     * 互耦识别（{@code _extract_coupled_pairs}）只认因果型。
     */
    public boolean isCausal() {
        return causal;
    }

    /**
     * 解析文本为关系类型（大小写不敏感、忽略首尾空白）。
     *
     * @return 合法的关系类型；为空或非法时返回 {@code null}
     */
    public static RelationTypeEnum from(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    /** 规范取值清单，用于错误提示。 */
    public static String supportedValues() {
        return "CAUSE, CAUSAL, CORRELATION, ASSOCIATION";
    }
}
