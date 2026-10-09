import { Money } from "../../shared/money/money";
import { LedgerDirection } from "../ledger/ledger-direction";
import { FailureCode } from "./failure-code";
import { WagerTransactionKind } from "./wager-transaction-kind";
import { WagerTransactionStatus } from "./wager-transaction-status";
import {
  InvalidTransactionStateError,
  InvalidWagerTransactionError,
} from "./wager-transaction.errors";
import {
  CreateWagerTransactionProps,
  WagerTransactionState,
} from "./wager-transaction.types";

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,

    /** ID da transação no provedor, não o ID interno. */
    public readonly referenceExternalTransactionId: string | undefined,

    public readonly createdAt: Date,

    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _resultingBalance?: Money,
  ) {}

  /** Nasce em PENDING. Valida a exigência de referência por kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (
      (props.kind === WagerTransactionKind.Refund ||
        props.kind === WagerTransactionKind.Rollback) &&
      !props.referenceExternalTransactionId?.trim()
    )
      throw new InvalidWagerTransactionError(
        `${props.kind} requires a reference transaction`,
      );

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      new Date(),
      WagerTransactionStatus.Pending,
    );
  }

  /**
   * Reconstructs persisted state without revalidating domain transitions.
   */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.resultingBalance,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get resultingBalance(): Money | undefined {
    return this._resultingBalance;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  markProcessed(
    referenceTransactionId: string | undefined,
    resultingBalance: Money,
    at: Date,
  ): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError(this.status);

    if (this.requiresReference() && !referenceTransactionId?.trim())
      throw new InvalidWagerTransactionError(
        `${this.kind} requires a resolved reference transaction`,
      );

    if (resultingBalance.currency !== this.money.currency) {
      throw new InvalidWagerTransactionError(
        "Resulting balance currency must match transaction currency",
      );
    }

    this._referenceTransactionId = referenceTransactionId;
    this._resultingBalance = resultingBalance;
    this._processedAt = at;
    this._status = WagerTransactionStatus.Processed;
  }

  markPendingReference(): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError(this.status);

    if (!this.requiresReference())
      throw new InvalidWagerTransactionError(
        `${this.kind} does not require a reference transaction`,
      );

    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError(this.status);

    this._failureCode = code;
    this._status = WagerTransactionStatus.Rejected;
  }

  fail(code: FailureCode): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError(this.status);

    this._failureCode = code;
    this._status = WagerTransactionStatus.Failed;
  }

  isTerminal(): boolean {
    return (
      this.status === WagerTransactionStatus.Processed ||
      this.status === WagerTransactionStatus.Rejected ||
      this.status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /**
   * Determines the ledger direction, reversing the referenced transaction for rollbacks.
   *
   * | Operation | Direction |
   * |-----------|-----------|
   * | OPENING | CREDIT |
   * | BET | DEBIT |
   * | WIN | CREDIT |
   * | LOSS | No entry |
   * | REFUND | CREDIT |
   * | ROLLBACK BET | CREDIT |
   * | ROLLBACK WIN | DEBIT |
   * | ROLLBACK REFUND | DEBIT |
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;

      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Rollback:
        if (!reference)
          throw new InvalidWagerTransactionError(
            "Rollback requires a reference transaction",
          );

        switch (reference.kind) {
          case WagerTransactionKind.Bet:
            return LedgerDirection.Credit;

          case WagerTransactionKind.Win:
          case WagerTransactionKind.Refund:
            return LedgerDirection.Debit;

          default:
            throw new InvalidWagerTransactionError(
              `Cannot rollback a ${reference.kind} transaction`,
            );
        }

      case WagerTransactionKind.Loss:
        throw new InvalidWagerTransactionError(
          "LOSS does not produce a ledger entry",
        );
    }
  }
}
