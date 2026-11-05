defmodule JobWorker.Repo do
  @moduledoc """
  Thin Postgrex wrapper over the same DB the Go services use.
  DSN: postgres://fund:fund@localhost:5432/fund (override with DATABASE_URL).
  """
  use GenServer

  @pool_size 5

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    {:ok, conn} = Postgrex.start_link(
      hostname: hostname(),
      username: "fund",
      password: "fund",
      database: "fund",
      port: 5432,
      pool_size: @pool_size
    )
    {:ok, conn}
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

  @doc "Run a query against the shared pool."
  def query!(sql, params \\ []) do
    GenServer.call(__MODULE__, {:query, sql, params}, 15_000)
  end

  @impl true
  def handle_call({:query, sql, params}, _from, conn) do
    {:reply, Postgrex.query(conn, sql, params), conn}
  end
end
