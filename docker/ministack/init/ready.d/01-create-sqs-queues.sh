
#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
DLQ_NAME="wager-transactions-dlq.fifo"
QUEUE_NAME="wager-transactions.fifo"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

cat > "$WORKDIR/dlq-attributes.json" <<'JSON'
{
  "FifoQueue": "true"
}
JSON

aws --endpoint-url "$ENDPOINT" sqs create-queue \
  --queue-name "$DLQ_NAME" \
  --attributes "file://$WORKDIR/dlq-attributes.json" \
  >/dev/null

DLQ_URL="$(
  aws --endpoint-url "$ENDPOINT" sqs get-queue-url \
    --queue-name "$DLQ_NAME" \
    --query QueueUrl \
    --output text
)"

DLQ_ARN="$(
  aws --endpoint-url "$ENDPOINT" sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text
)"

cat > "$WORKDIR/queue-attributes.json" <<JSON
{
  "FifoQueue": "true",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}"
}
JSON

aws --endpoint-url "$ENDPOINT" sqs create-queue \
  --queue-name "$QUEUE_NAME" \
  --attributes "file://$WORKDIR/queue-attributes.json" \
  >/dev/null

  cat > "$WORKDIR/events-attributes.json" <<'JSON'
{
  "FifoQueue": "true"
}
JSON

aws --endpoint-url "$ENDPOINT" sqs create-queue \
  --queue-name "wager-events.fifo" \
  --attributes "file://$WORKDIR/events-attributes.json" \
  >/dev/null

echo "SQS queues initialized successfully."
