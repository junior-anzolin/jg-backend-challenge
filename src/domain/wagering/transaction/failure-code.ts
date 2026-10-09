export enum FailureCode {
  InsufficientBalance = "INSUFFICIENT_BALANCE",
  ReversalWouldOverdraw = "REVERSAL_WOULD_OVERDRAW",
  ReferenceNotFound = "REFERENCE_NOT_FOUND",
  InvalidReference = "INVALID_REFERENCE",
  ReferenceAlreadyReversed = "REFERENCE_ALREADY_REVERSED",
  ReferenceAmountMismatch = "REFERENCE_AMOUNT_MISMATCH",
  CurrencyMismatch = "CURRENCY_MISMATCH",
  PermanentInfrastructureFailure = "PERMANENT_INFRASTRUCTURE_FAILURE",
}
