defmodule JobWorker.Submitter do
  @moduledoc """
  Submits one CREATED order to the downstream mock.
  Timeout/transport error maps to UNKNOWN — never FAILED.
  """

  def run(conn, order_id, request_id) do
    case fetch_order(conn, order_id) do
      {:ok, order} -> submit(conn, order, request_id)
      :skip -> :ok
    end
  end

  defp fetch_order(conn, order_id) do
    case Postgrex.query!(conn, """
          SELECT id, state, fund_code, direction, amount FROM orders WHERE id=$1
         """, [order_id]) do
      %{num_rows: 0} -> :skip
      %{rows: [[id, state, fund, dir, amount]]} ->
        # Idempotent handler: only CREATED orders move; re-delivery is a no-op.
        if state == "CREATED", do: {:ok, %{id: id, fund: fund, dir: dir, amount: amount}}, else: :skip
    end
  end

  defp submit(conn, order, request_id) do
    url = mock_url() <> "/submit"
    body = Jason.encode!(%{
      order_id: order.id, fund_code: order.fund,
      direction: order.dir, amount: order.amount, request_id: request_id
    })
    # httpc rejects lowercase header names; omit content-type, the mock ignores it
    result = :httpc.request(:post, {String.to_charlist(url), [], ~c"application/json", body},
                           [timeout: 800, connect_timeout: 800], [])
    handle_result(conn, order, request_id, result)
  end

  defp handle_result(conn, order, request_id, {:ok, {{_, 200, _}, _, resp_body}}) do
    %{"status" => status} = Jason.decode!(to_string(resp_body))
    case status do
      "ACCEPTED" -> trans(conn, order.id, :created, :submitted, "SUBMITTED", request_id)
      "REJECTED" -> trans(conn, order.id, :created, :failed, "REJECTED", request_id)
      _ -> trans(conn, order.id, :created, :unknown, "UNEXPECTED_RESPONSE", request_id)
    end
  end

  # Timeout or transport error: result is UNKNOWN, never FAILED.
  defp handle_result(conn, order, request_id, {:error, _reason}) do
    IO.warn("downstream timeout, order -> UNKNOWN (#{order.id})")
    trans(conn, order.id, :created, :unknown, "DOWNSTREAM_TIMEOUT", request_id)
  end

  defp trans(conn, order_id, from, to, event, request_id) do
    case JobWorker.OrderCore.transition(conn, order_id, from, to, event, request_id) do
      :ok -> :ok
      {:error, :conflict} -> :ok  # someone else moved it; fine under at-least-once
      {:error, reason} -> {:error, reason}
    end
  end

  defp mock_url, do: System.get_env("MOCK_URL", "http://localhost:8090")
end
