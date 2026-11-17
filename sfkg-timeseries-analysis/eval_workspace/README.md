# 合同验收评测工作区（重建版）

> **重建说明**：本工作区原位于 `~/gcad`，2026-10-26 被误删；2026-10-27 按持久化记忆
> 记录重建于项目目录内（`eval_workspace/`），`~/gcad` 现为其软链接（工厂冻结代码
> 硬编码 `DATA_ROOT=~/gcad/data`，软链接保证工厂代码不动）。

## 目录

```
eval_workspace/
├── data/
│   ├── SMD/machine-*-*/      # 28 台，OmniAnomaly 官方 txt 转换（锚点已校验）
│   ├── SMAP/<entity>/        # 54 实体，telemanom（HF 镜像）转换
│   ├── MSL/<entity>/         # 28 实体（含无标签干净通道 T-10），同上
│   ├── SWaT/AllInOne/        # train/test/test_label.npy（工厂布局）
│   ├── PSM/AllInOne/         # 同上
│   └── raw/                  # 原始下载物（telemanom_hf、RANSynCoders、SensitiveHUE）
├── gcad_repro/               # GCAD 官方仓库（Tc99m/GCAD, AAAI 2025）克隆
├── scripts/                  # 实验与转换脚本
└── results/                  # 报告、日志、指标卡
```

## 数据重建（全部公开来源）

| 数据集 | 来源 | 锚点校验 |
|---|---|---|
| SMD | [OmniAnomaly 仓库](https://github.com/smallcowbaby/OmniAnomaly)（仓库内 txt） | 1-1: 28479 点/2694 异常/8 事件 ✓ |
| SMAP/MSL | [telemanom HF 镜像](https://huggingface.co/datasets/appleparan/telemanom)（经 [hf-mirror.com](https://hf-mirror.com)） | D-3: 8640 点/3276 异常/37.92% ✓；54+28=82 实体 |
| PSM | [RANSynCoders 仓库](https://github.com/eBay/RANSynCoders)（train/test.csv） | 转换后核对 |
| SWaT | [SensitiveHUE 仓库](https://github.com/yuesuoqingqiu/SensitiveHUE)（train.npy/test.npz） | 转换后核对 |
| solar/electricity/traffic | [LSTNet 仓库](https://github.com/laiguokun/multivariate-time-series-data) | naive 基线复现 ✓ |

转换脚本：`scripts/convert_smd.py`、`scripts/convert_telemanom.py`（含锚点断言，
不过不放行）。

## 合同验收测试接口

```bash
# 环境（工厂冻结代码 + 评测数据根）
export SFKG_PROJECT_ROOT=~/TimeSeriesEngine/sfkg-timeseries-analysis
export GCAD_DATA_ROOT=~/gcad/data        # 可选，默认即此

# 异常检测冒烟：DBSCAN@SMD 1-1 段级（期望 R=1.000/P=0.955 PASS）
python scripts/acceptance_test.py anomaly

# 预测冒烟：CI PatchTST@SMD 2-4 m24 正常段 1 种子（~1h GPU）
python scripts/acceptance_test.py forecast
```

只调工厂冻结代码（`tools/evaluate_contract_benchmarks.py`、
`src/patchtst_forecaster.py`），不复制实现。

## 关键实验重跑

```bash
# 段级锁存合并规则网格（dbscan@SMD 1-1，A/B/C 三口径）
python scripts/dbscan_merge_rules.py > results/dbscan_merge_rules_2026-10-25.log 2>&1

# 1 步 naive 基线对照（solar/electricity/traffic/ETTh1）
python scripts/mase_pred1_naive.py > results/mase_pred1_naive_2026-10-25.log 2>&1

# CI 10 种子续跑（种子 3-9，GPU，~8h）
python scripts/mase_ci_verify_cont.py --seed-start 3 --seeds 10 \
  > results/mase_ci_verify_cont_2026-10-27.log 2>&1

# GCAD 研究线 whole-SMAP 重训 + D-3 协议验证（GPU）
python scripts/gcad_d3_retrain.py --device cuda > results/gcad_d3_retrain_2026-10-27.log 2>&1
```

运行约束（服务器与他人共享）：`taskset -c 0-19,40-59` + `OMP_NUM_THREADS=8`；
GPU 用 torch 枚举序号对准空闲卡（CUDA_VISIBLE_DEVICES 序号会漂移）。

## 结论文件

- `results/core_contract_status_2026-10-24.md` — 主报告 v2（含重建说明）
- `results/contract_acceptance_metrics_card_2026-10-23.md` — 指标卡（导师交付件）
- `results/multids_findings_2026-10-24.md` — 多数据集扫描发现
- `results/gcad_impl_verdict_and_questions_2026-10-24.md` — GCAD 实现核对与待问问题

## 诚实声明（沿用）

候选结果 ≠ 合同整体验收：锁定重跑验证可复现性、不消除选型偏差；口径 5 项
（公式/点级事件级/重复报警计数/平台还是 GCAD/数据集确认方式）待导师口头确认。
