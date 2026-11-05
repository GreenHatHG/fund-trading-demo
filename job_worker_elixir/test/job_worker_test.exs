defmodule JobWorkerTest do
  use ExUnit.Case
  doctest JobWorker

  test "greets the world" do
    assert JobWorker.hello() == :world
  end
end
