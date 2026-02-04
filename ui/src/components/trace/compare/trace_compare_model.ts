import { SpanSummary } from "../../../types";
import { mapSpanStage } from "../waterfall/waterfall_model";

export interface StageRollup {
  key: string;
  label: string;
  spanCount: number;
  totalDurationMs: number;
  totalSelfTimeMs: number;
}

export interface AlignedStage {
  key: string;
  label: string;
  left?: StageRollup;
  right?: StageRollup;
}

export function buildStageRollups(spans: SpanSummary[]): StageRollup[] {
  const stages = new Map<string, StageRollup>();
  const order: string[] = [];

  spans.forEach((span) => {
    const stage = mapSpanStage(span);
    if (!stages.has(stage.key)) {
      stages.set(stage.key, {
        key: stage.key,
        label: stage.label,
        spanCount: 0,
        totalDurationMs: 0,
        totalSelfTimeMs: 0
      });
      order.push(stage.key);
    }
    const entry = stages.get(stage.key)!;
    entry.spanCount += 1;
    entry.totalDurationMs += span.duration_ms;
    entry.totalSelfTimeMs += Number((span as any).self_time_ms ?? 0);
  });

  return order.map((key) => stages.get(key)!);
}

export function alignStages(left: StageRollup[], right: StageRollup[]): AlignedStage[] {
  const rightMap = new Map(right.map((stage) => [stage.key, stage]));
  const aligned: AlignedStage[] = [];

  left.forEach((stage) => {
    aligned.push({
      key: stage.key,
      label: stage.label,
      left: stage,
      right: rightMap.get(stage.key)
    });
    rightMap.delete(stage.key);
  });

  rightMap.forEach((stage) => {
    aligned.push({
      key: stage.key,
      label: stage.label,
      right: stage
    });
  });

  return aligned;
}
