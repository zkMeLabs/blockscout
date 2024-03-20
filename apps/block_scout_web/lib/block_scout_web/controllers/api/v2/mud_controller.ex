defmodule BlockScoutWeb.API.V2.MudController do
  use BlockScoutWeb, :controller

  import BlockScoutWeb.Chain,
    only: [
      next_page_params: 4,
      paging_options: 1,
      split_list_by_page: 1
    ]

  alias Explorer.Chain
  alias Explorer.Chain.Mud

  action_fallback(BlockScoutWeb.API.V2.FallbackController)

  @doc """
    Function to handle GET requests to `/api/v2/mud/worlds` endpoint.
  """
  @spec worlds(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def worlds(conn, _params) do
    worlds = Mud.worlds_list()

    conn
    |> put_status(200)
    |> render(:worlds, %{worlds: worlds})
  end

  @doc """
    Function to handle GET requests to `/api/v2/mud/worlds/count` endpoint.
  """
  @spec worlds_count(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def worlds_count(conn, _params) do
    count = Mud.worlds_count()

    conn
    |> put_status(200)
    |> render(:count, %{count: count})
  end

  def world_tables(conn, %{"world" => world_param} = _params) do
    with {:ok, world} <- Chain.string_to_address_hash(world_param) do
      tables = Mud.world_tables(world)

      conn
      |> put_status(200)
      |> render(:tables, %{tables: tables})
    end
  end

  def world_tables_count(conn, %{"world" => world_param} = _params) do
    with {:ok, world} <- Chain.string_to_address_hash(world_param) do
      count = Mud.world_tables_count(world)

      conn
      |> put_status(200)
      |> render(:count, %{count: count})
    end
  end

  def world_table_records(conn, %{"world" => world_param, "table_id" => table_id_param} = params) do
    with {:ok, world} <- Chain.string_to_address_hash(world_param),
         {:ok, table_id} <- Chain.string_to_transaction_hash(table_id_param),
         sort_by <- parse_sorting_order(params) do
      records =
        Mud.world_table_records(
          world,
          table_id,
          Map.get(params, "key0"),
          Map.get(params, "key1"),
          sort_by
        )

      conn
      |> put_status(200)
      |> render(:records, %{records: records})
    end
  end

  def world_table_records_count(conn, %{"world" => world_param, "table_id" => table_id_param} = _params) do
    with {:ok, world} <- Chain.string_to_address_hash(world_param),
         {:ok, table_id} <- Chain.string_to_transaction_hash(table_id_param) do
      count = Mud.world_table_records_count(world, table_id)

      conn
      |> put_status(200)
      |> render(:count, %{count: count})
    end
  end

  def world_table_record(
        conn,
        %{"world" => world_param, "table_id" => table_id_param, "record_id" => record_id_param} = _params
      ) do
    with {:ok, world} <- Chain.string_to_address_hash(world_param),
         {:ok, table_id} <- Chain.string_to_transaction_hash(table_id_param),
         {:ok, record_id} <- hex_string_to_binary(record_id_param) do
      record = Mud.world_table_record(world, table_id, record_id)

      conn
      |> put_status(200)
      |> render(:record, %{record: record})
    end
  end

  defp parse_sorting_order(params) do
    sort =
      case Map.get(params, "sort") do
        "desc" -> :desc
        _ -> :asc
      end

    sort_by =
      case Map.get(params, "sort_by") do
        "key0" -> :key0
        "key1" -> :key1
        _ -> :key_bytes
      end

    {sort, sort_by}
  end

  defp hex_string_to_binary("0x" <> hex) do
    Base.decode16(hex, case: :mixed)
  end

  defp hex_string_to_binary(hex) do
    Base.decode16(hex, case: :mixed)
  end
end
