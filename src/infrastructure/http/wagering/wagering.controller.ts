import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Headers,
  HttpCode,
  HttpException,
  HttpStatus,
  NotFoundException,
  Post,
  UnprocessableEntityException,
} from "@nestjs/common";

import {
  IdempotencyConflictError,
  InvalidWagerTransactionInputError,
  ProcessWagerTransactionUseCase,
  WagerTransactionAlreadyExistsError,
  WalletNotFoundError,
} from "../../../application/wagering/process-wager-transaction.use-case";
import { InvalidMoneyError } from "../../../domain/shared/money/money.errors";
import { WagerTransactionStatus } from "../../../domain/wagering/transaction/wager-transaction-status";
import { InvalidWagerTransactionError } from "../../../domain/wagering/transaction/wager-transaction.errors";
import { ProcessWagerTransactionDto } from "./dto/process-wager-transaction.dto";

@Controller("wagering/transactions")
export class WageringController {
  constructor(
    private readonly processWagerTransactionUseCase: ProcessWagerTransactionUseCase,
  ) {}

  @Post()
  @HttpCode(HttpStatus.OK)
  async process(
    @Body() body: ProcessWagerTransactionDto,
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Headers("x-correlation-id") correlationId: string | undefined,
    @Headers("x-causation-id") causationId: string | undefined,
  ) {
    if (
      typeof idempotencyKey !== "string" ||
      !idempotencyKey.trim() ||
      idempotencyKey.length > 255
    ) {
      throw new BadRequestException(
        "Idempotency-Key must be provided and contain at most 255 characters",
      );
    }

    try {
      const result = await this.processWagerTransactionUseCase.execute({
        ...body,
        idempotencyKey: idempotencyKey.trim(),
        correlationId,
        causationId,
      });

      if (result.status === WagerTransactionStatus.Rejected) {
        throw new UnprocessableEntityException(result);
      }

      if (
        result.status === WagerTransactionStatus.Pending ||
        result.status === WagerTransactionStatus.PendingReference
      ) {
        throw new HttpException(result, HttpStatus.ACCEPTED);
      }

      if (result.status === WagerTransactionStatus.Failed) {
        throw new HttpException(result, HttpStatus.INTERNAL_SERVER_ERROR);
      }

      return result;
    } catch (error) {
      if (
        error instanceof IdempotencyConflictError ||
        error instanceof WagerTransactionAlreadyExistsError
      ) {
        throw new ConflictException(error.message);
      }

      if (error instanceof WalletNotFoundError) {
        throw new NotFoundException(error.message);
      }

      if (
        error instanceof InvalidWagerTransactionInputError ||
        error instanceof InvalidMoneyError ||
        error instanceof InvalidWagerTransactionError
      ) {
        throw new BadRequestException(error.message);
      }

      throw error;
    }
  }
}
