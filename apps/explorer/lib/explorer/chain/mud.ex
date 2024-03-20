defmodule Explorer.Chain.Mud do
  @moduledoc """
    Represents a MUD framework database record.
  """
  use Explorer.Schema

  import Ecto.Query,
    only: [
      from: 2,
      where: 3,
      order_by: 3
    ]

  alias ABI.{TypeDecoder, TypeEncoder}
  alias Explorer.{Chain, PagingOptions, Repo, SortingHelper}

  alias Explorer.Chain.{
    Mud,
    Mud.Schema,
    Mud.Schema.FieldSchema,
    Hash
  }

  require Logger

  @default_paging_options %PagingOptions{page_size: 50}
  @schema_prefix "mud"

  @store_tables_table_id Base.decode16!("746273746f72650000000000000000005461626c657300000000000000000000", case: :lower)

  # https://github.com/latticexyz/mud/blob/cc4f4246e52982354e398113c46442910f9b04bb/packages/store/src/codegen/tables/Tables.sol#L34-L42
  @store_tables_table_schema %Schema{
    key_schema: FieldSchema.from("0x002001005f000000000000000000000000000000000000000000000000000000"),
    value_schema: FieldSchema.from("0x006003025f5f5fc4c40000000000000000000000000000000000000000000000"),
    key_names: ["tableId"],
    value_names: ["fieldLayout", "keySchema", "valueSchema", "abiEncodedKeyNames", "abiEncodedValueNames"]
  }

  @primary_key false
  typed_schema "records" do
    field(:address, Hash.Address, null: false)
    field(:table_id, Hash.Full, null: false)
    field(:key_bytes, :binary)
    field(:key0, Hash.Full)
    field(:key1, Hash.Full)
    field(:static_data, :binary)
    field(:encoded_lengths, :binary)
    field(:dynamic_data, :binary)
    field(:is_deleted, :boolean, null: false)
    field(:block_number, :decimal, null: false)
    field(:log_index, :decimal, null: false)
  end

  def enabled? do
    Application.get_env(:explorer, __MODULE__)[:enabled]
  end

  def worlds_list do
    from(r in Mud,
      select: r.address,
      distinct: true
    )
    |> Repo.Mud.all()
  end

  def worlds_count do
    from(r in Mud,
      select: r.address,
      distinct: true
    )
    |> Repo.Mud.aggregate(:count)
  end

  def world_tables(world) do
    table_ids =
      from(r in Mud,
        select: r.table_id,
        distinct: true,
        where: r.address == ^world
      )
      |> Repo.Mud.all()

    schemas = fetch_schemas(world, table_ids)

    table_ids
    |> Enum.map(fn table_id -> table_id |> decode_table_id() |> Map.put(:schema, format_schema(schemas[table_id])) end)
  end

  def world_tables_count(world) do
    from(r in Mud,
      select: r.table_id,
      distinct: true,
      where: r.address == ^world
    )
    |> Repo.Mud.aggregate(:count)
  end

  def world_table_records(world, table_id, key0, key1, sort_by) do
    schema = fetch_schemas(world, [table_id])[table_id]

    records =
      from(r in Mud,
        where: r.address == ^world and r.table_id == ^table_id
      )
      |> apply_filter(:key0, key0, schema, 0)
      |> apply_filter(:key1, key1, schema, 1)
      |> apply_sort_by(sort_by)
      |> Repo.Mud.all()

    table_id
    |> decode_table_id()
    |> Map.merge(%{
      schema: format_schema(schema),
      records: records |> Enum.map(fn r -> r |> decode_record(schema) |> format_record(r) end)
    })
  end

  def world_table_records_count(world, table_id) do
    from(r in Mud,
      where: r.address == ^world and r.table_id == ^table_id
    )
    |> Repo.Mud.aggregate(:count)
  end

  def world_table_record(world, table_id, record_id) do
    record =
      from(r in Mud,
        where: r.address == ^world and r.table_id == ^table_id and r.key_bytes == ^record_id
      )
      |> Repo.Mud.one()

    schema = fetch_schemas(world, [table_id])[table_id]

    table_id
    |> decode_table_id()
    |> Map.merge(%{
      schema: format_schema(schema),
      record: record && record |> decode_record(schema) |> format_record(record)
    })
  end

  defp apply_filter(query, _key_name, nil, _schema, _field_idx), do: query

  defp apply_filter(query, key_name, key, schema, field_idx) do
    type_hint = schema && FieldSchema.type_of(schema.key_schema, field_idx)
    key_enc =
      case key do
        "false" ->
          <<0::256>>

        "true" ->
          <<1::256>>

        "0x" <> hex when type_hint == 97 ->
          bin = Base.decode16!(hex, case: :mixed)
          <<0::size(256 - byte_size(bin) * 8), bin::binary>>

        "0x" <> hex ->
          bin = Base.decode16!(hex, case: :mixed)
          <<bin::binary, 0::size(256 - byte_size(bin) * 8)>>

        dec ->
          num = Integer.parse(dec) |> elem(0)
          <<num::256>>
      end

    case key_name do
      :key0 -> query |> where([r], r.key0 == ^key_enc)
      :key1 -> query |> where([r], r.key1 == ^key_enc)
    end
  end

  defp apply_sort_by(query, sort_options), do: query |> order_by([r], ^sort_options)

  def format_schema(nil), do: nil

  def format_schema(schema) do
    %{
      key_names: schema.key_names,
      key_types: decode_type_names(schema.key_schema),
      value_names: schema.value_names,
      value_types: decode_type_names(schema.value_schema)
    }
  end

  def format_record(decoded_record, record) do
    id = "0x" <> Base.encode16(record.key_bytes, case: :lower)

    %{
      id: id,
      decoded: decoded_record,
      raw: %{
        key_bytes: id,
        static_data: "0x" <> Base.encode16(record.static_data || <<>>, case: :lower),
        encoded_lengths: "0x" <> Base.encode16(record.encoded_lengths || <<>>, case: :lower),
        dynamic_data: "0x" <> Base.encode16(record.dynamic_data || <<>>, case: :lower),
        block_number: record.block_number,
        log_index: record.log_index
      },
      is_deleted: record.is_deleted
    }
  end

  def fetch_schemas(world, table_ids) do
    from(r in Mud,
      where: r.address == ^world and r.table_id == ^@store_tables_table_id and r.key0 in ^table_ids
    )
    |> Repo.Mud.all()
    |> Enum.into(%{}, fn r ->
      schema_record = decode_record(r, @store_tables_table_schema).value

      schema = %Schema{
        key_schema: schema_record["keySchema"] |> FieldSchema.from(),
        value_schema: schema_record["valueSchema"] |> FieldSchema.from(),
        key_names: schema_record["abiEncodedKeyNames"] |> decode_abi_encoded_strings(),
        value_names: schema_record["abiEncodedValueNames"] |> decode_abi_encoded_strings()
      }

      {r.key0, schema}
    end)
  end

  defp decode_abi_encoded_strings("0x" <> hex_encoded) do
    hex_encoded
    |> Base.decode16!(case: :mixed)
    |> ABI.TypeDecoder.decode_raw([{:array, :string}])
    |> Enum.at(0)
  end

  def decode_record(_record, nil), do: %{}

  def decode_record(record, schema) do
    value =
      if record.is_deleted do
        nil
      else
        decode_fields(
          record.static_data,
          record.encoded_lengths,
          record.dynamic_data,
          schema.value_names,
          schema.value_schema
        )
      end

    %{
      key: decode_key_tuple(record.key_bytes, schema.key_names, schema.key_schema),
      value: value
    }
  end

  defp decode_type_names(layout_schema) do
    {_, types} = decode_types(layout_schema)
    types |> Enum.map(&encode_type_name/1)
  end

  defp decode_types(layout_schema) do
    static_fields_count = :binary.at(layout_schema.word, 2)
    dynamic_fields_count = :binary.at(layout_schema.word, 3)

    {static_fields_count, :binary.bin_to_list(layout_schema.word, 4, static_fields_count + dynamic_fields_count)}
  end

  def decode_key_tuple(key_bytes, fields, layout_schema) do
    {_, types} = decode_types(layout_schema)

    fields
    |> Enum.zip(types)
    |> Enum.reduce({%{}, key_bytes}, fn {field, type}, {acc, data} ->
      type_size = static_type_size(type)
      <<word::binary-size(32), rest::binary>> = data

      enc =
        if type < 64 or type >= 96 do
          :binary.part(word, 32 - type_size, type_size)
        else
          :binary.part(word, 0, type_size)
        end

      decoded = decode_type(type, enc)

      {Map.put(acc, field, decoded), rest}
    end)
    |> elem(0)
  end

  def decode_fields(static_data, encoded_lengths, dynamic_data, fields, layout_schema) do
    {static_fields_count, types} = decode_types(layout_schema)

    {static_types, dynamic_types} = Enum.split(types, static_fields_count)

    {static_fields, dynamic_fields} = Enum.split(fields, static_fields_count)

    res =
      static_fields
      |> Enum.zip(static_types)
      |> Enum.reduce({%{}, static_data}, fn {field, type}, {acc, data} ->
        type_size = static_type_size(type)
        <<enc::binary-size(type_size), rest::binary>> = data
        decoded = decode_type(type, enc)

        {Map.put(acc, field, decoded), rest}
      end)
      |> elem(0)

    dynamic_type_lengths =
      if encoded_lengths == nil or byte_size(encoded_lengths) == 0 do
        []
      else
        encoded_lengths
        |> :binary.bin_to_list(0, 25)
        |> Enum.chunk_every(5)
        |> Enum.reverse()
        |> Enum.map(&(&1 |> :binary.list_to_bin() |> :binary.decode_unsigned()))
      end

    [dynamic_fields, dynamic_types, dynamic_type_lengths]
    |> Enum.zip()
    |> Enum.reduce({res, dynamic_data}, fn {field, type, length}, {acc, data} ->
      <<enc::binary-size(length), rest::binary>> = data
      decoded = decode_type(type, enc)

      {Map.put(acc, field, decoded), rest}
    end)
    |> elem(0)
  end

  def static_type_size(type) do
    case type do
      _ when type < 97 -> rem(type, 32) + 1
      97 -> 20
      _ -> 0
    end
  end

  def decode_type(type, raw) do
    case type do
      _ when type < 32 ->
        raw |> :binary.decode_unsigned() |> Integer.to_string()

      _ when type < 64 ->
        size = static_type_size(type)
        <<int::signed-integer-size(size * 8)>> = raw
        int |> Integer.to_string()

      _ when type < 96 or type == 97 or type == 196 ->
        "0x" <> Base.encode16(raw, case: :lower)

      96 ->
        raw == <<1>>

      _ when type < 196 ->
        raw
        |> :binary.bin_to_list()
        |> Enum.chunk_every(static_type_size(type - 98))
        |> Enum.map(&decode_type(type - 98, :binary.list_to_bin(&1)))

      197 ->
        raw

      _ ->
        raise "Unknown type: #{type}"
    end
  end

  def encode_type_name(type) do
    case type do
      _ when type < 32 -> "uint" <> Integer.to_string((type + 1) * 8)
      _ when type < 64 -> "int" <> Integer.to_string((type - 31) * 8)
      _ when type < 96 -> "bytes" <> Integer.to_string((type - 63) * 8)
      96 -> "bool"
      97 -> "address"
      _ when type < 196 -> encode_type_name(type - 98) <> "[]"
      196 -> "bytes"
      197 -> "string"
      _ -> "unknown_type_" <> Integer.to_string(type)
    end
  end

  defp decode_table_id(%Hash{byte_count: 32, bytes: raw} = table_id) do
    <<prefix::binary-size(2), namespace::binary-size(14), table_name::binary-size(16)>> = raw

    trimmed_namespace = String.trim_trailing(namespace, "\u0000")
    trimmed_table_name = String.trim_trailing(table_name, "\u0000")

    table_full_name =
      if String.length(trimmed_namespace) > 0 do
        prefix <> "." <> trimmed_namespace <> "." <> trimmed_table_name
      else
        prefix <> "." <> trimmed_table_name
      end

    table_type =
      case prefix do
        "ot" -> "offchain"
        "tb" -> "onchain"
        _ -> "unknown"
      end

    %{
      table_id: table_id,
      table_full_name: table_full_name,
      table_type: table_type,
      table_namespace: trimmed_namespace,
      table_name: trimmed_table_name
    }
  end
end
