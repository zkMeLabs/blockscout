defmodule Explorer.ExchangeRates.Source.DefiLlama do
  @moduledoc """
  Adapter for fetching exchange rates from https://defillama.com/

  """

  alias Explorer.ExchangeRates.Source

  @behaviour Source

  @impl Source
  def format_data(_), do: []

  @spec history_url(non_neg_integer()) :: String.t()
  def history_url(_previous_days) do
    "#{base_url()}/historicalChainTvl"
  end

  @impl Source
  def source_url do
    ""
  end

  @impl Source
  def source_url(_) do
    ""
  end

  @impl Source
  def headers do
    []
  end

  @doc """
  Converts date time string into DateTime object formatted as date
  """
  @spec date(String.t()) :: Date.t()
  def date(date_time_string) do
    with {:ok, datetime, _} <- DateTime.from_iso8601(date_time_string) do
      datetime
      |> DateTime.to_date()
    end
  end

  defp base_url do
    base_free_url()
  end

  defp base_free_url do
    config(:base_url) || "https://api.llama.fi/v2"
  end

  @spec config(atom()) :: term
  defp config(key) do
    Application.get_env(:explorer, __MODULE__, [])[key]
  end
end
