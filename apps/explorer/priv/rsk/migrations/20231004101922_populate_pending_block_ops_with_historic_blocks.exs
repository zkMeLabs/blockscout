defmodule Explorer.Repo.RSK.Migrations.PopulatePendingBlockOpsWithHistoricBlocks do
  use Ecto.Migration

  def change do
    execute(
      """
        INSERT INTO pending_block_operations
        SELECT hash, NOW(), NOW(), number
        FROM blocks
        WHERE consensus IS TRUE;
      """,
      """
        DELETE FROM pending_block_operations;
      """
    )
  end
end
