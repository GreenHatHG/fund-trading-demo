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

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_opts) do
    # Reach into the pool process to grab the raw Postgrex conn for transactions.
    # (Single-node demo shortcut; a real app would share a connection pool.)
    conn = :sys.get_state(JobWorker.Repo)
    schedule()
    {:ok, conn}
  end

  @impl true
  def handle_info(:tick, conn) do
    run_once(conn)
    schedule()
    {:noreply, conn}
  end

  defp schedule, do: Process.send_after(self(), :tick, @tick)

  defp run_once(conn) do
    # Sweep first so an idle queue still advances stale SUBMITTED orders.
    sweep_stale(conn)

    for _ <- 1..@batch do
      case JobWorker.Jobs.claim_next(conn) do
        :empty -> :done
        {:ok, job} -> dispatch(conn, job)
      end
    end
  end

  defp dispatch(conn, %{type: "SUBMIT"} = job) do
    request_id = "job-submit-#{job.order_id}"
    task = Task.Supervisor.async_nolink(JobWorker.TaskSupervisor, fn ->
      JobWorker.Submitter.run(conn, job.order_id, request_id)
    end)

    case Task.yield(task, 5_000) || Task.shutdown(task, :brutal_kill) do
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
  end

  # RECONCILE jobs belong to the reconciliation component; no-op here.
  defp dispatch(conn, %{type: "RECONCILE"} = job), do: JobWorker.Jobs.finish(conn, job.id)

  defp sweep_stale(conn) do
    ids = JobWorker.Jobs.stale_submitted(conn, @sweep_after_s, 100)
    for id <- ids do
      case JobWorker.OrderCore.transition(conn, id, :submitted, :unknown, "TIMEOUT_SWEEP", "sweep") do
        :ok -> :ok
        {:error, :conflict} -> :ok
        {:error, reason} -> Logger.warning("sweep #{id}: #{inspect(reason)}")
      end
    end
  end
end
