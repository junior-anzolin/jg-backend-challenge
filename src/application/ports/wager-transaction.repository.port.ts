import { WagerTransaction } from "../../domain/wagering/transaction/wager-transaction";
import { WagerTransactionKind } from "../../domain/wagering/transaction/wager-transaction-kind";

export interface WagerTransactionRepositoryPort {
  findById(id: string): Promise<WagerTransaction | null>;

  findByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<WagerTransaction | null>;

  findByProviderAndExternalTransactionId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null>;

  findReversalByReferenceAndKind(
    referenceTransactionId: string,
    kind: WagerTransactionKind.Refund | WagerTransactionKind.Rollback,
  ): Promise<WagerTransaction | null>;

  findDuePendingReferences(
    now: Date,
    limit: number,
  ): Promise<WagerTransaction[]>;

  save(transaction: WagerTransaction): Promise<void>;
}
