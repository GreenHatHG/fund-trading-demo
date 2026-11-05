defmodule JobWorker.Application do
  @moduledoc "Supervision tree: DB pool + task supervisor + poller."
  use Application

  @impl true
  def start(_type, _args) do
    # Start the pool first; the poller needs the raw conn for transactions.
    children = [
      JobWorker.Repo,
      {Task.Supervisor, name: JobWorker.TaskSupervisor},
      {JobWorker.Poller, :ok}
    ]

    opts = [strategy: :one_for_one, name: JobWorker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
