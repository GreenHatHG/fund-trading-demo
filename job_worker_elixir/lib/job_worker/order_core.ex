defmodule JobWorker.OrderCore do
  @moduledoc """
  Elixir port of internal/ordercore: the single owner of order state transitions.
  Same table as the Go side; Transition does optimistic-lock update + event
  append in one transaction. No state write anywhere else.
  """

  @states %{
    created: "CREATED", submitted: "SUBMITTED",
    confirmed: "CONFIRMED", failed: "FAILED", unknown: "UNKNOWN"
  }

  # from -> allowed to-states. Mirrors internal/ordercore/ordercore.go.
  @allowed %{
    created: MapSet.new([:submitted, :failed, :unknown]),
    submitted: MapSet.new([:confirmed, :failed, :unknown]),
    unknown: MapSet.new([:confirmed, :failed])
  }

  def state_name(atom), do: Map.fetch!(@states, atom)

  def can_transition?(from, to), do: MapSet.member?(Map.fetch!(@allowed, from), to)

  @doc """
  Move the order from -> to with optimistic locking and append an event.
  Returns :ok | {:error, :conflict} | {:error, :invalid_transition} | {:error, reason}.
  """
  def transition(conn, order_id, from, to, event_type, request_id) do
    if can_transition?(from, to) do
      Postgrex.transaction(conn, fn tx ->
        case Postgrex.query!(tx, """
              UPDATE orders SET state=$1, version=version+1, updated_at=now()
              WHERE id=$2 AND state=$3
                AND version=(SELECT version FROM orders WHERE id=$2)
             """, [state_name(to), order_id, state_name(from)]) do
          %{num_rows: 0} -> Postgrex.rollback(tx, :conflict)
          %{num_rows: 1} ->
            Postgrex.query!(tx,
              "INSERT INTO order_events (order_id, event_type, payload, request_id) VALUES ($1,$2,$3::jsonb,$4)",
              [order_id, event_type, "{}", request_id])
        end
      end)
      |> case do
        {:ok, _} -> :ok
        {:error, :conflict} -> {:error, :conflict}
        {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} -> {:error, :invalid_transition}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :invalid_transition}
    end
  end
end
