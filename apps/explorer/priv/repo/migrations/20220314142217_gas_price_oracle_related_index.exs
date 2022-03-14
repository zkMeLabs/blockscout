defmodule Explorer.Repo.Migrations.GasPriceOracleRelatedIndex do
  use Ecto.Migration

  def change do
    create(
      index(:transactions, [:block_number],
        where: ~s["status" = 1 AND "gas_price" > 0],
        name: :successful_transactions_with_positive_gas_price
      )
    )
  end
end
