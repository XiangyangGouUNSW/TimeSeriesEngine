package com.sfkg.timeseries.mapper;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesRelation;

/**
 * 大规模存储回归：验证 LocalJsonTableStore 在数百条记录下的
 * writeAll / upsert / update / 跨实例 reload 的持久化正确性。
 */
class LargeScaleStorageTests {

    @Test
    void largeScaleCategoryUpsertUpdateAndReload(@TempDir Path dir) {
        Path file = dir.resolve("timeseries-category.json");
        LocalJsonTableStore<TimeseriesCategory> store = new LocalJsonTableStore<>(file, TimeseriesCategory.class);

        int n = 500;
        List<TimeseriesCategory> categories = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            categories.add(category("project-" + (i % 5), "cat-" + i, "Category " + i));
        }
        store.writeAll(categories);
        assertEquals(n, store.readAll().size(), "writeAll 后应读取到全部记录");

        // 同业务键 upsert → 覆盖而不是新增
        TimeseriesCategory updated = category("project-0", "cat-0", "Updated category 0");
        store.upsert(c -> updated.getProjectId().equals(c.getProjectId())
                && updated.getCategoryId().equals(c.getCategoryId()), updated);
        assertEquals(n, store.readAll().size(), "upsert 同键不应产生重复记录");

        // update 原地修改
        store.update(c -> "cat-1".equals(c.getCategoryId()), c -> c.setCategoryName("Renamed"));
        List<TimeseriesCategory> after = store.readAll();
        assertEquals(n, after.size());
        assertEquals("Updated category 0", findByCat(after, "cat-0").getCategoryName());
        assertEquals("Renamed", findByCat(after, "cat-1").getCategoryName());

        // 用新的 store 实例从磁盘重载 → 持久化内容正确
        LocalJsonTableStore<TimeseriesCategory> reloaded = new LocalJsonTableStore<>(file, TimeseriesCategory.class);
        List<TimeseriesCategory> reloadedAll = reloaded.readAll();
        assertEquals(n, reloadedAll.size());
        assertEquals("Updated category 0", findByCat(reloadedAll, "cat-0").getCategoryName());
        assertEquals("Renamed", findByCat(reloadedAll, "cat-1").getCategoryName());
        // 未修改的记录保持不变
        assertEquals("Category 42", findByCat(reloadedAll, "cat-42").getCategoryName());
    }

    @Test
    void largeScaleConstraintsAndRelationsRoundTripWithRichFields(@TempDir Path dir) {
        Path constraintFile = dir.resolve("timeseries-constraint.json");
        Path relationFile = dir.resolve("timeseries-relation.json");
        LocalJsonTableStore<TimeseriesConstraint> constraintStore =
                new LocalJsonTableStore<>(constraintFile, TimeseriesConstraint.class);
        LocalJsonTableStore<TimeseriesRelation> relationStore =
                new LocalJsonTableStore<>(relationFile, TimeseriesRelation.class);

        int constraintCount = 200;
        List<TimeseriesConstraint> constraints = new ArrayList<>();
        for (int i = 0; i < constraintCount; i++) {
            TimeseriesConstraint c = new TimeseriesConstraint();
            c.setProjectId("p" + (i % 4));
            c.setConstraintId("c-" + i);
            c.setConstraintName("constraint " + i);
            c.setVariableMapping(Map.of("x", "seq-" + i));
            c.setConstraintExpression("x <= " + i);
            c.setLowerBound(0.0);
            c.setUpperBound((double) i);
            c.setEffectiveStatus("ENABLE");
            c.setConfirmStatus("CONFIRMED");
            c.setCreateTime(LocalDateTime.now());
            c.setUpdateTime(LocalDateTime.now());
            constraints.add(c);
        }
        constraintStore.writeAll(constraints);
        assertEquals(constraintCount, constraintStore.readAll().size());

        int relationCount = 150;
        List<TimeseriesRelation> relations = new ArrayList<>();
        for (int i = 0; i < relationCount; i++) {
            TimeseriesRelation r = new TimeseriesRelation();
            r.setProjectId("p" + (i % 4));
            r.setRelationId("r-" + i);
            r.setRelationName("relation " + i);
            r.setSourceSequences(List.of("src-" + i));
            r.setTargetSequenceId("tgt-" + i);
            r.setRelationType("CAUSE");
            r.setLagRange("0m-10m");
            r.setConfidence(new BigDecimal("0.5"));
            r.setEffectiveStatus("ENABLE");
            r.setConfirmStatus("CONFIRMED");
            r.setCreateTime(LocalDateTime.now());
            r.setUpdateTime(LocalDateTime.now());
            relations.add(r);
        }
        relationStore.writeAll(relations);
        assertEquals(relationCount, relationStore.readAll().size());

        // 跨实例 reload：数量一致 + 富字段（Double / BigDecimal / LocalDateTime）无损
        List<TimeseriesConstraint> loadedConstraints =
                new LocalJsonTableStore<>(constraintFile, TimeseriesConstraint.class).readAll();
        List<TimeseriesRelation> loadedRelations =
                new LocalJsonTableStore<>(relationFile, TimeseriesRelation.class).readAll();
        assertEquals(constraintCount, loadedConstraints.size());
        assertEquals(relationCount, loadedRelations.size());

        TimeseriesConstraint c42 = loadedConstraints.stream()
                .filter(c -> "c-42".equals(c.getConstraintId())).findFirst().orElseThrow();
        assertEquals(42.0, c42.getUpperBound());
        assertEquals("ENABLE", c42.getEffectiveStatus());
        assertEquals(Map.of("x", "seq-42"), c42.getVariableMapping());
        assertNotNull(c42.getCreateTime());

        TimeseriesRelation r42 = loadedRelations.stream()
                .filter(r -> "r-42".equals(r.getRelationId())).findFirst().orElseThrow();
        assertEquals(new BigDecimal("0.5"), r42.getConfidence());
        assertEquals("CAUSE", r42.getRelationType());
        assertEquals("0m-10m", r42.getLagRange());
        assertEquals(List.of("src-42"), r42.getSourceSequences());
        assertNotNull(r42.getUpdateTime());
    }

    @Test
    void largeScaleWriteIsDeterministicAcrossReloads(@TempDir Path dir) {
        Path file = dir.resolve("timeseries-relation.json");
        LocalJsonTableStore<TimeseriesRelation> store = new LocalJsonTableStore<>(file, TimeseriesRelation.class);

        int n = 300;
        for (int i = 0; i < n; i++) {
            final int idx = i;
            TimeseriesRelation r = new TimeseriesRelation();
            r.setProjectId("p");
            r.setRelationId("r-" + idx);
            r.setRelationName("relation " + idx);
            r.setSourceSequences(List.of("src-" + idx));
            r.setTargetSequenceId("tgt-" + idx);
            r.setRelationType("CORRELATION");
            r.setConfidence(new BigDecimal("0." + (idx % 10)));
            r.setEffectiveStatus("ENABLE");
            r.setConfirmStatus("CONFIRMED");
            store.upsert(existing -> existing.getProjectId().equals("p")
                    && existing.getRelationId().equals("r-" + idx), r);
        }
        List<TimeseriesRelation> first = store.readAll();
        List<TimeseriesRelation> second =
                new LocalJsonTableStore<>(file, TimeseriesRelation.class).readAll();
        assertEquals(n, first.size());
        assertEquals(n, second.size());
        for (int i = 0; i < n; i++) {
            assertEquals(first.get(i).getRelationId(), second.get(i).getRelationId(),
                    "reload 后记录顺序/内容应稳定");
            assertEquals(first.get(i).getConfidence(), second.get(i).getConfidence());
        }
        assertTrue(first.stream().anyMatch(r -> "r-123".equals(r.getRelationId())));
    }

    private TimeseriesCategory category(String projectId, String categoryId, String name) {
        TimeseriesCategory c = new TimeseriesCategory();
        c.setProjectId(projectId);
        c.setCategoryId(categoryId);
        c.setCategoryName(name);
        c.setConfirmStatus("CONFIRMED");
        return c;
    }

    private TimeseriesCategory findByCat(List<TimeseriesCategory> list, String categoryId) {
        return list.stream()
                .filter(c -> categoryId.equals(c.getCategoryId()))
                .findFirst()
                .orElseThrow();
    }
}
