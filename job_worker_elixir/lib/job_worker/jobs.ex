defmodule JobWorker.Jobs do
  @moduledoc """
  Job queue access: enqueue SUBMIT for CREATED orders, claim (SKIP LOCKED),
  finish, retry with backoff.
  """

  # Producer loop: one active SUBMIT job per CREATED order. Submitter is
  # idempotent (only CREATED orders move), so at-least-once enqueue is safe.
  def enqueue_submits(conn, limit) do
    %{num_rows: n} = Postgrex.query!(conn, """
      INSERT INTO jobs (order_id, job_type, state)
      SELECT id, 'SUBMIT', 'PENDING' FROM orders
      WHERE state='CREATED'
      ORDER BY id LIMIT $1
      ON CONFLICT (order_id) WHERE state IN ('PENDING','RUNNING') DO NOTHING
     """, [limit])
    n
  end

  # A worker crash leaves jobs RUNNING forever, and the active-index then
  # blocks re-enqueue for that order. Requeue stale RUNNING so it can retry.
  def requeue_stale_running(conn, older_than_s) do
    cutoff = DateTime.add(DateTime.utc_now(), -older_than_s, :second)
    %{num_rows: n} = Postgrex.query!(conn, """
      UPDATE jobs SET state='PENDING', updated_at=now()
      WHERE state='RUNNING' AND updated_at < $1
     """, [cutoff])
    n
  end

  def claim_next(conn) do
    case Postgrex.query!(conn, """
          UPDATE jobs SET state='RUNNING', attempts=attempts+1, updated_at=now()
          WHERE id = (SELECT id FROM jobs
                      WHERE state='PENDING' AND run_after<=now()
                      ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
          RETURNING id, order_id, job_type, attempts, max_attempts
         """, []) do
      %{num_rows: 0} -> :empty
      %{rows: [[id, order_id, type, attempts, max_attempts]]} ->
        {:ok, %{id: id, order_id: order_id, type: type, attempts: attempts, max_attempts: max_attempts}}
    end
  end

  def finish(conn, id), do:
    Postgrex.query!(conn, "UPDATE jobs SET state='DONE', updated_at=now() WHERE id=$1", [id])

  def retry(conn, %{id: id, attempts: attempts, max_attempts: max}) when attempts >= max do
    Postgrex.query!(conn, "UPDATE jobs SET state='DEAD', updated_at=now() WHERE id=$1", [id])
  end
  def retry(conn, %{id: id, attempts: attempts}) do
    # exponential backoff: 2^attempts seconds
    backoff_s = :math.pow(2, attempts) |> trunc()
    run_after = DateTime.add(DateTime.utc_now(), backoff_s, :second)
    Postgrex.query!(conn, """
      UPDATE jobs SET state='PENDING', run_after=$1, updated_at=now()
      WHERE id=$2
     """, [run_after, id])
  end

  def stale_submitted(conn, older_than_s, limit) do
    cutoff = DateTime.add(DateTime.utc_now(), -older_than_s, :second)
    %{rows: rows} = Postgrex.query!(conn, """
      SELECT id, version FROM orders
      WHERE state='SUBMITTED' AND updated_at < $1
      LIMIT $2
     """, [cutoff, limit])
    rows
  end
end
