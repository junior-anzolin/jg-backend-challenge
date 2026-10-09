export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
}

export interface InboxMessageState extends ReceiveInboxProps {
  receivedAt: Date;
  processedAt?: Date;
}
