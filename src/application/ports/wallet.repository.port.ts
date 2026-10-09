import { Wallet } from "../../domain/wagering/wallet/wallet";

export interface WalletRepositoryPort {
  findById(id: string): Promise<Wallet | null>;

  /**
   * Loads a wallet with a write lock held until the current transaction ends.
   */
  findByIdForUpdate(id: string): Promise<Wallet | null>;

  findByPlayerIdAndCurrency(
    playerId: string,
    currency: string,
  ): Promise<Wallet | null>;

  save(wallet: Wallet): Promise<void>;
}
