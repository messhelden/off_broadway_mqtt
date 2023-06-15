defmodule OffBroadway.MQTT.AcknowledgerTest do
  use OffBroadway.MQTTCase, async: true

  import ExUnit.CaptureLog
  import OffBroadway.MQTT

  alias OffBroadway.MQTT.Acknowledger, as: Ack
  alias OffBroadway.MQTT.Error
  alias OffBroadway.MQTT.Queue

  @moduletag start_registry: true
  @moduletag start_queue: true

  describe "requeuing messages" do
    test "requeues messages that failed with a retry error", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%Error{message: "Argh!", ack: :retry})
        |> then(&Broadway.Message.configure_ack(&1, on_failure: :requeue))

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert [%{status: :ok}] = Queue.dequeue(queue, 1)
    end

    test "does not requeue succeeded messages", %{
      queue: queue,
      queue_topic: topic
    } do
      succ_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)

      assert :ok = Ack.ack(topic, [succ_msg], [])
      assert [] = Queue.dequeue(queue, 1)
    end

    test "does not requeue messages with a ignore error", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%Error{message: "Argh!", ack: :ignore})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert [] = Queue.dequeue(queue, 1)
    end

    test "does not requeue messages with a other error #1", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%Error{message: "Argh!", ack: :foo})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert [] = Queue.dequeue(queue, 1)
    end

    test "does not requeue messages with a other error #2", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%RuntimeError{message: "Argh!"})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert [] = Queue.dequeue(queue, 1)
    end

    test "does not requeue messages with a other error #3", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg({:error, :foo})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert [] = Queue.dequeue(queue, 1)
    end

    test "does not requeue messages with a other error #4", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg("hell froze solid")

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert [] = Queue.dequeue(queue, 1)
    end
  end

  describe "Logging errors" do
    test "does nothing if no messages" do
      assert "" ==
               capture_log(fn ->
                 assert :ok = Ack.ack("foo", [], [])
               end)
    end

    test "logs exception messages on error level", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%Error{message: "Argh!"})

      log =
        capture_log([level: :error], fn ->
          assert :ok = Ack.ack(topic, [], [failed_msg])
        end)

      assert log =~ "Argh!"
    end

    test "logs any exception's messages on error level", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%RuntimeError{message: "Argh!"})

      log =
        capture_log([level: :error], fn ->
          assert :ok = Ack.ack(topic, [], [failed_msg])
        end)

      assert log =~ "Argh!"
    end

    test "logs any error tuple messages on error level", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg({:error, :foo})

      log =
        capture_log([level: :error], fn ->
          assert :ok = Ack.ack(topic, [], [failed_msg])
        end)

      assert log =~ "foo"
    end

    test "logs any error messages on error level", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg("this did not work")

      log =
        capture_log([level: :error], fn ->
          assert :ok = Ack.ack(topic, [], [failed_msg])
        end)

      assert log =~ "this did not work"
    end

    test "logs retry exception messages on warn level", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%Error{message: "Argh!", ack: :retry})

      log =
        capture_log([level: :warn], fn ->
          assert :ok = Ack.ack(topic, [], [failed_msg])
        end)

      assert log =~ "Argh!"
    end

    test "logs skip exception messages on info level", %{
      queue: queue,
      queue_topic: topic
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%Error{message: "Argh!", ack: :ignore})

      log =
        capture_log([level: :debug], fn ->
          assert :ok = Ack.ack(topic, [], [failed_msg])
        end)

      assert log =~ "Argh!"
    end
  end
end
