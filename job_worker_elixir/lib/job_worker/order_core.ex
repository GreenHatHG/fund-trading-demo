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
  expected_version is the version the caller read; the update only lands if
  the row still has it. Returns :ok | {:error, :conflict} |
  {:error, :invalid_transition} | {:error, reason}.
  """
  def transition(conn, order_id, from, to, event_type, request_id, expected_version) do
    with :ok <- check_transition(from, to),
         {:ok, _} <- run_in_tx(conn, order_id, from, to, event_type, request_id, expected_version) do
      :ok
    else
      {:error, %Postgrex.Error{postgres: %{code: :check_violation}}} -> {:error, :invalid_transition}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_transition(from, to) do
    if can_transition?(from, to), do: :ok, else: {:error, :invalid_transition}
  end

  defp run_in_tx(conn, order_id, from, to, event_type, request_id, expected_version) do
    Postgrex.transaction(conn, fn tx ->
      update_state(tx, order_id, from, to, expected_version)
      append_event(tx, order_id, event_type, request_id)
      :ok
    end)
  end

  # Optimistic lock: if the row moved on since the caller read it, the
  # version check fails and the update hits 0 rows.
  defp update_state(tx, order_id, from, to, expected_version) do
    %{num_rows: 1} =
      Postgrex.query!(tx, """
        UPDATE orders SET state=$1, version=version+1, updated_at=now()
        WHERE id=$2 AND state=$3 AND version=$4
       """, [state_name(to), order_id, state_name(from), expected_version])
    :ok
  rescue
    _ in MatchError -> Postgrex.rollback(tx, :conflict)
  end

  defp append_event(tx, order_id, event_type, request_id) do
    Postgrex.query!(tx,
      "INSERT INTO order_events (order_id, event_type, payload, request_id) VALUES ($1,$2,$3::jsonb,$4)",
      [order_id, event_type, "{}", request_id])
    :ok
  end
end
