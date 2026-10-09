import { DomainError } from "../errors/domain.error";

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super(message);
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, received: string) {
    super(`Currency mismatch: expected ${expected}, received ${received}`);
  }
}
