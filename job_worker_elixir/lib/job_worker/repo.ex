defmodule JobWorker.Repo do
  @moduledoc """
  Postgrex connection pool over the same DB the Go services use.
  The pool pid is stored in :persistent_term so Poller can grab it without a
  GenServer hop. DSN: postgres://fund:fund@localhost:5432/fund (override with
  DATABASE_URL).
  """
  use GenServer

  @default_pool_size 5
  @key :job_worker_repo_pool

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    {:ok, pool} = Postgrex.start_link(
      hostname: hostname(),
      username: "fund",
      password: "fund",
      database: "fund",
      port: 5432,
      # Pool size must cover the Poller's batch concurrency or tasks queue here.
      pool_size: pool_size(),
      # Pool checkout must not outlive the caller's own timeout below.
      timeout: 15_000
    )
    :persistent_term.put(@key, pool)
    {:ok, pool}
  end

  defp pool_size do
    case System.get_env("POOL_SIZE") do
      nil -> @default_pool_size
      s -> String.to_integer(s)
    end
  end

  defp hostname do
    case System.get_env("DATABASE_URL") do
      nil -> "localhost"
      url ->
        # postgres://user:pass@host:port/db
        case Regex.run(~r{//([^:/@]+)}, url) do
          [_, host] -> host
          _ -> "localhost"
        end
    end
  end

  @doc "The shared pool pid; pass it anywhere Postgrex expects a conn."
  def pool, do: :persistent_term.get(@key)

  @doc "Run a query on the shared pool (checks out a connection per call)."
  def query!(sql, params \\ []) do
    Postgrex.query!(pool(), sql, params)
  end
end
