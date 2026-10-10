import { Transform, Type } from "class-transformer";
import {
  IsIn,
  IsNotEmpty,
  IsString,
  IsUUID,
  Matches,
  MaxLength,
  ValidateIf,
  ValidateNested,
} from "class-validator";

import { WagerTransactionKind } from "../../../../domain/wagering/transaction/wager-transaction-kind";

const trimString = ({ value }: { value: unknown }) =>
  typeof value === "string" ? value.trim() : value;

export class MoneyDto {
  @Transform(trimString)
  @IsString()
  @Matches(/^\d{1,18}\.\d{2}$/, {
    message: "amount must have up to 18 integer digits and 2 decimal places",
  })
  amount!: string;

  @Transform(trimString)
  @IsString()
  @Matches(/^[A-Z]{3}$/, {
    message: "currency must be a 3-letter uppercase currency code",
  })
  currency!: string;
}

export class ProcessWagerTransactionDto {
  @Transform(trimString)
  @IsString()
  @IsNotEmpty()
  @MaxLength(128)
  providerId!: string;

  @Transform(trimString)
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  externalTransactionId!: string;

  @Transform(trimString)
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  playerId!: string;

  @Transform(trimString)
  @IsUUID()
  walletId!: string;

  @Transform(trimString)
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  roundId!: string;

  @Transform(trimString)
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  gameId!: string;

  @IsIn([
    WagerTransactionKind.Bet,
    WagerTransactionKind.Win,
    WagerTransactionKind.Loss,
    WagerTransactionKind.Refund,
    WagerTransactionKind.Rollback,
  ])
  kind!: WagerTransactionKind;

  @ValidateNested()
  @Type(() => MoneyDto)
  money!: MoneyDto;

  @ValidateIf(
    (dto: ProcessWagerTransactionDto) =>
      dto.kind === WagerTransactionKind.Refund ||
      dto.kind === WagerTransactionKind.Rollback ||
      dto.referenceExternalTransactionId !== undefined,
  )
  @Transform(trimString)
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  referenceExternalTransactionId?: string;
}
