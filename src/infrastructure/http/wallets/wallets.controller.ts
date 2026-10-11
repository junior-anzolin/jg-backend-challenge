import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
} from "@nestjs/common";

import {
  CreateWalletUseCase,
  WalletAlreadyExistsError,
} from "../../../application/wallets/create-wallet.use-case";
import { GetWalletLedgerUseCase } from "../../../application/wallets/get-wallet-ledger.use-case";
import { GetWalletUseCase } from "../../../application/wallets/get-wallet.use-case";
import { ReconcileWalletUseCase } from "../../../application/wallets/reconcile-wallet.use-case";
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
    private readonly getWalletLedgerUseCase: GetWalletLedgerUseCase,
    private readonly reconcileWalletUseCase: ReconcileWalletUseCase,
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

  @Get(":walletId/ledger")
  async getLedger(
    @Param("walletId", new ParseUUIDPipe()) walletId: string,
    @Query("cursor") cursor?: string,
    @Query("limit") limitRaw?: string,
  ) {
    const limit = limitRaw === undefined ? 50 : Number(limitRaw);

    if (
      (limitRaw !== undefined && !/^\d+$/.test(limitRaw)) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 100
    ) {
      throw new BadRequestException(
        "limit must be an integer between 1 and 100",
      );
    }

    if (cursor !== undefined && !cursor.trim()) {
      throw new BadRequestException("cursor cannot be empty");
    }

    try {
      const page = await this.getWalletLedgerUseCase.execute(walletId, {
        ...(cursor !== undefined ? { cursor } : {}),
        limit,
      });

      if (!page) {
        throw new NotFoundException("Wallet not found");
      }

      return {
        walletId,
        items: page.items.map((entry) => ({
          id: entry.id,
          walletId: entry.walletId,
          transactionId: entry.transactionId,
          direction: entry.direction,
          money: entry.money.toJSON(),
          balanceBefore: entry.balanceBefore.toJSON(),
          balanceAfter: entry.balanceAfter.toJSON(),
          createdAt: entry.createdAt.toISOString(),
        })),
        nextCursor: page.nextCursor ?? null,
      };
    } catch (error) {
      if (error instanceof RangeError) {
        throw new BadRequestException(error.message);
      }

      throw error;
    }
  }

  @Post(":walletId/reconciliation")
  @HttpCode(HttpStatus.OK)
  async reconcile(@Param("walletId", new ParseUUIDPipe()) walletId: string) {
    const result = await this.reconcileWalletUseCase.execute(walletId);

    if (!result) {
      throw new NotFoundException("Wallet not found");
    }

    return result;
  }
}
