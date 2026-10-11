import {
  BadRequestException,
  Controller,
  Get,
  NotFoundException,
  Param,
} from "@nestjs/common";

import { GetWagerTransactionUseCase } from "../../../application/wagering/get-wager-transaction.use-case";

@Controller("providers/:providerId/wagering/transactions")
export class ProviderWageringTransactionsController {
  constructor(
    private readonly getWagerTransactionUseCase: GetWagerTransactionUseCase,
  ) {}

  @Get(":externalTransactionId")
  async getByProviderAndExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ) {
    if (
      !providerId.trim() ||
      providerId.length > 128 ||
      !externalTransactionId.trim() ||
      externalTransactionId.length > 255
    ) {
      throw new BadRequestException("Invalid transaction identifiers");
    }

    const transaction =
      await this.getWagerTransactionUseCase.executeByProviderAndExternalId(
        providerId,
        externalTransactionId,
      );

    if (!transaction) {
      throw new NotFoundException("Wager transaction not found");
    }

    return transaction;
  }
}
