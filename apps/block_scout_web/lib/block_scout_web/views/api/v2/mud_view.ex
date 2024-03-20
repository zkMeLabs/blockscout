defmodule BlockScoutWeb.API.V2.MudView do
  use BlockScoutWeb, :view

  alias Explorer.Chain.{Block, Transaction}
  alias Explorer.Chain.ZkSync.TransactionBatch

  @doc """
    Function to render GET requests to `/api/v2/mud/worlds` endpoint.
  """
  @spec render(binary(), map()) :: map() | non_neg_integer()
  def render("worlds.json", %{worlds: worlds}) do
    %{
      items: worlds,
      next_page_params: nil
    }
  end

  @doc """
    Function to render GET requests to `/api/v2/mud/worlds/count` endpoint.
  """
  @spec render(binary(), map()) :: map() | non_neg_integer()
  def render("count.json", %{count: count}) do
    %{
      count: count
    }
  end

  @spec render(binary(), map()) :: map() | non_neg_integer()
  def render("tables.json", %{tables: tables}) do
    %{
      tables: tables
    }
  end

  @spec render(binary(), map()) :: map() | non_neg_integer()
  def render("records.json", %{records: records}) do
    %{
      records: records
    }
  end

  @spec render(binary(), map()) :: map() | non_neg_integer()
  def render("record.json", %{record: record}) do
    record
  end
end
