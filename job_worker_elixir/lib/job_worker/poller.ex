defmodule JobWorker.Poller do
  @moduledoc """
  The loop: every @tick ms, claim due jobs and process each in a supervised
  Task (Task.Supervisor — a crashed submit can't kill the poller), then sweep
  stale SUBMITTED orders to UNKNOWN. Retry with exponential backoff on failure.
  """
  use GenServer
  require Logger

  @tick 200
  @batch 10
  @sweep_after_s 5
  @stale_running_s 30
  @enqueue_batch 100

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_opts) do
    # Always read the live pool via Repo.pool(); a cached pid would die on a
    # pool restart. Never store the pool in GenServer state.
    schedule()
    {:ok, nil}
  end

  @impl true
  def handle_info(:tick, state) do
    run_once()
    schedule()
    {:noreply, state}
  end

  defp schedule, do: Process.send_after(self(), :tick, @tick)

  defp run_once do
    conn = JobWorker.Repo.pool()
    # Sweep first so an idle queue still advances stale SUBMITTED orders.
    sweep_stale(conn)
    # Fill the queue before claiming so freshly CREATED orders are seen,
    # and unstick jobs orphaned in RUNNING by a crashed worker.
    enqueue_submits(conn)
    requeue_stale_running(conn)

    # Fire the whole batch, then collect: a slow submit no longer serializes
    # the rest of the batch behind it.
    submits =
      for _ <- 1..@batch do
        case JobWorker.Jobs.claim_next(conn) do
          :empty -> nil
          {:ok, job} -> job
        end
      end
      |> Enum.reject(&is_nil/1)

    collect(conn, Enum.map(submits, fn job -> {job, dispatch(job)} end))
  end

  defp dispatch(%{type: "SUBMIT"} = job) do
    request_id = "job-submit-#{job.order_id}"
    # Submitter takes the pool pid; Postgrex checks out a connection per
    # query, so concurrent tasks only queue on the pool — safe.
    Task.Supervisor.async_nolink(JobWorker.TaskSupervisor, fn ->
      JobWorker.Submitter.run(JobWorker.Repo.pool(), job.order_id, request_id)
    end)
  end

  defp collect(_conn, []), do: :ok

  defp collect(conn, tasks) do
    # yield_many returns [{task, result}] pairs; map each back to its job.
    job_of = Map.new(tasks, fn {j, t} -> {t.ref, j} end)

    tasks
    |> Enum.map(fn {_, t} -> t end)
    |> Task.yield_many(5_000)
    |> Enum.each(fn {task, result} ->
      job = Map.fetch!(job_of, task.ref)
      case result || Task.shutdown(task, :brutal_kill) do
        {:ok, :ok} -> JobWorker.Jobs.finish(conn, job.id)
        {:ok, {:error, reason}} ->
          Logger.warning("job #{job.id} failed: #{inspect(reason)}")
          JobWorker.Jobs.retry(conn, job)
        {:ok, :skip} -> JobWorker.Jobs.finish(conn, job.id)
        nil -> JobWorker.Jobs.retry(conn, job)  # timed out hard
        {:exit, reason} ->
          Logger.warning("job #{job.id} crashed: #{inspect(reason)}")
          JobWorker.Jobs.retry(conn, job)
      end
    end)
  end

  defp enqueue_submits(conn) do
    JobWorker.Jobs.enqueue_submits(conn, @enqueue_batch)
  rescue
    e -> Logger.warning("enqueue submits: #{Exception.message(e)}")
  end

  defp requeue_stale_running(conn) do
    case JobWorker.Jobs.requeue_stale_running(conn, @stale_running_s) do
      0 -> :ok
      n -> Logger.warning("requeued #{n} stale RUNNING job(s)")
    end
  rescue
    e -> Logger.warning("requeue stale running: #{Exception.message(e)}")
  end

  defp sweep_stale(conn) do
    ids = JobWorker.Jobs.stale_submitted(conn, @sweep_after_s, 100)
    for [id, version] <- ids do
      case JobWorker.OrderCore.transition(conn, id, :submitted, :unknown, "TIMEOUT_SWEEP", "sweep", version) do
        :ok -> :ok
        {:error, :conflict} -> :ok
        {:error, reason} -> Logger.warning("sweep #{id}: #{inspect(reason)}")
      end
    end
  end
end
