defmodule Explorer.Chain.Mud.Schema do
  @moduledoc """
    Represents a MUD framework database record schema.
  """

  defmodule FieldSchema do
    @moduledoc """
      Represents a MUD framework database record field schema.
    """

    defstruct [:word]

    @type t :: %__MODULE__{
            word: <<_::256>>
          }

    @spec from(binary()) :: :error | t()
    def from(<<bin::binary-size(32)>>), do: %__MODULE__{word: bin}

    def from("0x" <> <<hex::binary-size(64)>>) do
      with {:ok, bin} <- Base.decode16(hex, case: :mixed) do
        %__MODULE__{word: bin}
      end
    end

    def from(_), do: :error

    def type_of(%FieldSchema{word: word}, index), do: :binary.at(word, index + 4)
  end

  @enforce_keys [:key_schema, :value_schema, :key_names, :value_names]
  defstruct [:key_schema, :value_schema, :key_names, :value_names]
end
