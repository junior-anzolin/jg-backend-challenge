import { DomainError } from "../../shared/errors/domain.error";

export class InvalidLedgerEntryError extends DomainError {
  constructor(message: string) {
    super(message);
  }
}
