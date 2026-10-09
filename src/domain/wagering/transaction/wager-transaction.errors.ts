import { DomainError } from "../../shared/errors/domain.error";
import { WagerTransactionStatus } from "./wager-transaction-status";

export class InvalidWagerTransactionError extends DomainError {
  constructor(message: string) {
    super(message);
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(status: WagerTransactionStatus) {
    super(`Cannot transition transaction from terminal state: ${status}`);
  }
}
