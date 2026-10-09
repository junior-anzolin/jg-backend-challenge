import { DomainError } from '../../shared/errors/domain.error';

export class InsufficientBalanceError extends DomainError {
  constructor() {
    super('Insufficient wallet balance');
  }
}