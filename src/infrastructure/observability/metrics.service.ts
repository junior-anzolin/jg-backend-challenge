import { Injectable } from "@nestjs/common";

type MetricLabels = Record<string, string>;

type CounterSample = {
  labels: MetricLabels;
  value: number;
};

type HistogramSample = {
  count: number;
  sum: number;
  buckets: Map<number, number>;
};

const PROCESSING_DURATION_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
];

const METRIC_DEFINITIONS = [
  {
    name: "wager_transactions_total",
    help: "Total number of wager transactions by status.",
    type: "counter",
  },
  {
    name: "wager_duplicates_total",
    help: "Total number of duplicate wager requests or messages detected.",
    type: "counter",
  },
  {
    name: "wager_retries_total",
    help: "Total number of retry attempts.",
    type: "counter",
  },
  {
    name: "wager_dlq_messages_total",
    help: "Total number of messages sent to a dead-letter queue.",
    type: "counter",
  },
  {
    name: "wager_lock_conflicts_total",
    help: "Total number of database lock conflicts.",
    type: "counter",
  },
  {
    name: "wager_outbox_lag_seconds",
    help: "Age in seconds of the oldest unpublished outbox message.",
    type: "gauge",
  },
  {
    name: "wager_processing_duration_seconds",
    help: "Duration of wager transaction processing in seconds.",
    type: "histogram",
  },
] as const;

@Injectable()
export class MetricsService {
  private readonly counters = new Map<string, CounterSample[]>();
  private readonly histogram: HistogramSample = {
    count: 0,
    sum: 0,
    buckets: new Map(PROCESSING_DURATION_BUCKETS.map((bucket) => [bucket, 0])),
  };

  private outboxLagSeconds = 0;

  constructor() {
    this.outboxLagSeconds = 0;
  }

  recordTransactionStatus(status: string): void {
    this.incrementCounter("wager_transactions_total", { status });
  }

  recordDuplicate(): void {
    this.incrementCounter("wager_duplicates_total");
  }

  recordRetry(): void {
    this.incrementCounter("wager_retries_total");
  }

  recordDlqMessage(): void {
    this.incrementCounter("wager_dlq_messages_total");
  }

  recordLockConflict(): void {
    this.incrementCounter("wager_lock_conflicts_total");
  }

  observeProcessingDuration(durationMilliseconds: number): void {
    if (!Number.isFinite(durationMilliseconds) || durationMilliseconds < 0) {
      return;
    }

    const durationSeconds = durationMilliseconds / 1000;

    this.histogram.count++;
    this.histogram.sum += durationSeconds;

    for (const bucket of PROCESSING_DURATION_BUCKETS) {
      if (durationSeconds <= bucket) {
        this.histogram.buckets.set(
          bucket,
          (this.histogram.buckets.get(bucket) ?? 0) + 1,
        );
      }
    }
  }

  setOutboxLag(seconds: number): void {
    if (!Number.isFinite(seconds) || seconds < 0) {
      return;
    }

    this.outboxLagSeconds = seconds;
  }

  render(): string {
    const lines: string[] = [];

    for (const definition of METRIC_DEFINITIONS) {
      lines.push(`# HELP ${definition.name} ${definition.help}`);
      lines.push(`# TYPE ${definition.name} ${definition.type}`);

      if (definition.type === "counter") {
        const samples = this.counters.get(definition.name) ?? [];

        for (const sample of samples) {
          lines.push(
            `${definition.name}${this.formatLabels(sample.labels)} ${sample.value}`,
          );
        }

        continue;
      }

      if (definition.type === "gauge") {
        lines.push(
          `${definition.name} ${this.formatNumber(this.outboxLagSeconds)}`,
        );

        continue;
      }

      for (const bucket of PROCESSING_DURATION_BUCKETS) {
        lines.push(
          `${definition.name}_bucket${this.formatLabels({
            le: String(bucket),
          })} ${this.histogram.buckets.get(bucket) ?? 0}`,
        );
      }

      lines.push(
        `${definition.name}_bucket${this.formatLabels({
          le: "+Inf",
        })} ${this.histogram.count}`,
      );
      lines.push(
        `${definition.name}_sum ${this.formatNumber(this.histogram.sum)}`,
      );
      lines.push(`${definition.name}_count ${this.histogram.count}`);
    }

    return `${lines.join("\n")}\n`;
  }

  private incrementCounter(name: string, labels: MetricLabels = {}): void {
    const samples = this.counters.get(name) ?? [];
    const normalizedLabels = this.normalizeLabels(labels);
    const existing = samples.find(
      (sample) =>
        this.labelsKey(sample.labels) === this.labelsKey(normalizedLabels),
    );

    if (existing) {
      existing.value++;
    } else {
      samples.push({
        labels: normalizedLabels,
        value: 1,
      });
    }

    this.counters.set(name, samples);
  }

  private normalizeLabels(labels: MetricLabels): MetricLabels {
    return Object.fromEntries(
      Object.entries(labels).sort(([left], [right]) =>
        left.localeCompare(right),
      ),
    );
  }

  private labelsKey(labels: MetricLabels): string {
    return JSON.stringify(labels);
  }

  private formatLabels(labels: MetricLabels): string {
    const entries = Object.entries(labels);

    if (entries.length === 0) {
      return "";
    }

    const formatted = entries.map(
      ([key, value]) => `${key}="${this.escapeLabel(value)}"`,
    );

    return `{${formatted.join(",")}}`;
  }

  private escapeLabel(value: string): string {
    return value
      .replace(/\\/g, "\\\\")
      .replace(/\n/g, "\\n")
      .replace(/"/g, '\\"');
  }

  private formatNumber(value: number): string {
    return Number(value.toFixed(9)).toString();
  }
}
