package com.sfkg.timeseries.service;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.DerivedBinaryExpressionDTO;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.DerivedExpressionDTO;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.DerivedSeriesConfigItem;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.LinearCombinationDTO;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.LinearTermDTO;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/** Checks derived-series expressions before they are synchronized to Core. */
@Component
public class DerivedSeriesConfigValidator {

    private static final Set<String> OPERATORS = Set.of("ADD", "SUBTRACT", "MULTIPLY", "DIVIDE");

    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;

    public DerivedSeriesConfigValidator(TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager) {
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
    }

    public void validate(String projectId, List<DerivedSeriesConfigItem> items) {
        if (items == null || items.isEmpty()) {
            throw new BusinessException("derived series items must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        Set<String> derivedIds = new HashSet<>();
        for (DerivedSeriesConfigItem item : items) {
            if (item == null || item.getDerivedSequenceId() == null || item.getDerivedSequenceId().isBlank()) {
                throw new BusinessException("derivedSequenceId must not be empty");
            }
            String id = item.getDerivedSequenceId().trim();
            if (!derivedIds.add(id)) {
                throw new BusinessException("duplicate derivedSequenceId: " + id);
            }
            if (memoryCache.getInstanceBySequenceId(projectId, id) != null) {
                throw new BusinessException("derivedSequenceId conflicts with an instance sequence: " + id);
            }
            if ((item.getLinearCombination() == null) == (item.getExpression() == null)) {
                throw new BusinessException("derived series must provide exactly one expression form: " + id);
            }
        }

        Map<String, Set<String>> dependencies = new HashMap<>();
        for (DerivedSeriesConfigItem item : items) {
            Set<String> sources = new HashSet<>();
            if (item.getLinearCombination() != null) {
                validateLinearCombination(projectId, item.getDerivedSequenceId(), item.getLinearCombination(), derivedIds, sources);
            } else {
                validateExpression(projectId, item.getDerivedSequenceId(), item.getExpression(), derivedIds, sources);
            }
            dependencies.put(item.getDerivedSequenceId(), sources);
        }
        validateNoCycle(dependencies);
    }

    private void validateLinearCombination(String projectId, String derivedId,
            LinearCombinationDTO combination, Set<String> derivedIds, Set<String> sources) {
        if (combination.getTerms() == null || combination.getTerms().isEmpty()) {
            throw new BusinessException("linear combination terms must not be empty: " + derivedId);
        }
        if (combination.getBias() != null && !Double.isFinite(combination.getBias())) {
            throw new BusinessException("linear combination bias must be finite: " + derivedId);
        }
        for (LinearTermDTO term : combination.getTerms()) {
            if (term == null || term.getSequenceId() == null || term.getSequenceId().isBlank()) {
                throw new BusinessException("linear term sequenceId must not be empty: " + derivedId);
            }
            if (term.getCoefficient() == null || !Double.isFinite(term.getCoefficient())) {
                throw new BusinessException("linear term coefficient must be finite: " + term.getSequenceId());
            }
            validateSource(projectId, derivedId, term.getSequenceId(), derivedIds, sources);
        }
    }

    private void validateExpression(String projectId, String derivedId, DerivedExpressionDTO expression,
            Set<String> derivedIds, Set<String> sources) {
        if (expression == null) {
            throw new BusinessException("derived expression must not be null: " + derivedId);
        }
        int forms = (expression.getSequenceId() != null && !expression.getSequenceId().isBlank() ? 1 : 0)
                + (expression.getConstant() != null ? 1 : 0)
                + (expression.getBinary() != null ? 1 : 0);
        if (forms != 1) {
            throw new BusinessException("derived expression node must contain exactly one form: " + derivedId);
        }
        if (expression.getSequenceId() != null && !expression.getSequenceId().isBlank()) {
            validateSource(projectId, derivedId, expression.getSequenceId(), derivedIds, sources);
            return;
        }
        if (expression.getConstant() != null) {
            if (!Double.isFinite(expression.getConstant())) {
                throw new BusinessException("derived expression constant must be finite: " + derivedId);
            }
            return;
        }
        DerivedBinaryExpressionDTO binary = expression.getBinary();
        if (binary.getOperator() == null || !OPERATORS.contains(binary.getOperator().trim().toUpperCase())
                || binary.getLeft() == null || binary.getRight() == null) {
            throw new BusinessException("invalid derived binary expression: " + derivedId);
        }
        if ("DIVIDE".equalsIgnoreCase(binary.getOperator()) && isZeroConstant(binary.getRight())) {
            throw new BusinessException("derived expression cannot divide by constant 0: " + derivedId);
        }
        validateExpression(projectId, derivedId, binary.getLeft(), derivedIds, sources);
        validateExpression(projectId, derivedId, binary.getRight(), derivedIds, sources);
    }

    private boolean isZeroConstant(DerivedExpressionDTO expression) {
        return expression != null && expression.getConstant() != null && expression.getConstant() == 0D;
    }

    private void validateSource(String projectId, String derivedId, String sourceId,
            Set<String> derivedIds, Set<String> sources) {
        String normalized = sourceId.trim();
        if (normalized.equals(derivedId)) {
            throw new BusinessException("derived series cannot reference itself: " + derivedId);
        }
        if (derivedIds.contains(normalized)) {
            sources.add(normalized);
            return;
        }
        TimeseriesInstanceConfig instance = memoryCache.getInstanceBySequenceId(projectId, normalized);
        if (instance == null) {
            throw new BusinessException("derived source sequence not found: " + normalized);
        }
        String dataType = instance.getDataType();
        if (!"double".equalsIgnoreCase(dataType) && !"int64".equalsIgnoreCase(dataType)) {
            throw new BusinessException("derived source must be numeric: " + normalized);
        }
    }

    private void validateNoCycle(Map<String, Set<String>> dependencies) {
        Set<String> visiting = new HashSet<>();
        Set<String> visited = new HashSet<>();
        for (String id : dependencies.keySet()) {
            visit(id, dependencies, visiting, visited);
        }
    }

    private void visit(String id, Map<String, Set<String>> dependencies,
            Set<String> visiting, Set<String> visited) {
        if (visited.contains(id)) {
            return;
        }
        if (!visiting.add(id)) {
            throw new BusinessException("derived series contains a cycle involving: " + id);
        }
        for (String dependency : dependencies.getOrDefault(id, Set.of())) {
            visit(dependency, dependencies, visiting, visited);
        }
        visiting.remove(id);
        visited.add(id);
    }
}
