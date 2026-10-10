import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  Headers,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Post,
} from "@nestjs/common";

import {
  CreateWalletUseCase,
  WalletAlreadyExistsError,
} from "../../../application/wallets/create-wallet.use-case";
import { GetWalletUseCase } from "../../../application/wallets/get-wallet.use-case";
import { InvalidMoneyError } from "../../../domain/shared/money/money.errors";

interface CreateWalletRequest {
  playerId?: unknown;
  initialBalance?: {
    amount?: unknown;
    currency?: unknown;
  };
}

@Controller("wallets")
export class WalletsController {
  constructor(
    private readonly createWalletUseCase: CreateWalletUseCase,
    private readonly getWalletUseCase: GetWalletUseCase,
  ) {}

  @Post()
  async create(
    @Body() body: CreateWalletRequest,
    @Headers("x-correlation-id") correlationId?: string,
  ) {
    if (
      !body ||
      typeof body.playerId !== "string" ||
      !body.playerId.trim() ||
      !body.initialBalance ||
      typeof body.initialBalance.amount !== "string" ||
      typeof body.initialBalance.currency !== "string"
    ) {
      throw new BadRequestException(
        "playerId and initialBalance.amount/currency are required",
      );
    }

    try {
      const wallet = await this.createWalletUseCase.execute({
        playerId: body.playerId,
        initialBalance: {
          amount: body.initialBalance.amount,
          currency: body.initialBalance.currency,
        },
        correlationId,
      });

      return {
        id: wallet.id,
        playerId: wallet.playerId,
        balance: wallet.balance.toJSON(),
        version: wallet.version,
      };
    } catch (error) {
      if (error instanceof WalletAlreadyExistsError) {
        throw new ConflictException(error.message);
      }

      if (error instanceof InvalidMoneyError) {
        throw new BadRequestException(error.message);
      }

      throw error;
    }
  }

  @Get(":walletId")
  async getById(@Param("walletId", new ParseUUIDPipe()) walletId: string) {
    const wallet = await this.getWalletUseCase.execute(walletId);

    if (!wallet) {
      throw new NotFoundException("Wallet not found");
    }

    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }
}
