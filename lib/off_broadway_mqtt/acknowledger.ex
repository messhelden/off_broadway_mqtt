defmodule OffBroadway.MQTT.Acknowledger do
  @moduledoc """
  Implements `Broadway.Acknowledger` behaviour.
  """

  require Logger

  alias OffBroadway.MQTT.Config
  alias OffBroadway.MQTT.Telemetry

  @behaviour Broadway.Acknowledger

  @type ack_data :: %{
          queue: GenServer.name(),
          tries: non_neg_integer,
          on_failure: :ack | :requeue | nil,
          config: Config.t()
        }

  @impl Broadway.Acknowledger
  def ack(topic, successful, failed) do
    Logger.metadata(topic: topic)
    ack_messages(successful, topic, :success)
    ack_messages(failed, topic, :failed)
    :ok
  end

  @impl Broadway.Acknowledger
  def configure(_channel, ack_data, options) do
    Enum.each(options, fn
      {:on_failure, val} ->
        assert_valid_ack_option!(:on_failure, val)

      {other, _value} ->
        raise ArgumentError, "unsupported configure option #{inspect(other)}"
    end)

    ack_data = Map.merge(ack_data, Map.new(options))
    {:ok, ack_data}
  end

  defp ack_messages(messages, _topic, :failed) do
    Enum.each(messages, fn msg ->
      msg
      |> send_telemetry_event()
      |> log_failure()
      |> maybe_requeue()
    end)

    :ok
  end

  defp ack_messages([], _topic, _status), do: :ok

  defp ack_messages(messages, topic, :success) do
    Enum.each(messages, &send_telemetry_event/1)

    Logger.debug(
      "Successfully processed #{length(messages)} messages on #{inspect(topic)}"
    )

    :ok
  end

  @valid_ack_values [:ack, :requeue]

  defp assert_valid_ack_option!(name, value) do
    unless value in @valid_ack_values do
      raise ArgumentError,
            "unsupported value for #{inspect(name)} option: #{inspect(value)}"
    end
  end

  defp log_failure(
         %{
           status: {_, %{__exception__: true} = e, _exception_args},
           metadata: metadata
         } = message
       ) do
    log_metadata = Enum.into(metadata, [])
    log_failure_for_exception(e, log_metadata)
    message
  end

  defp log_failure(
         %{
           status: {_, reason},
           metadata: metadata
         } = message
       ) do
    log_metadata = Enum.into(metadata, [])

    Logger.error(
      "Processing message failed with unhandled reason: #{inspect(reason)}",
      log_metadata
    )

    message
  end

  defp log_failure_for_exception(%mod{ack: :ignore} = e, metadata) do
    Logger.debug(mod.message(e), metadata)
  end

  defp log_failure_for_exception(%mod{ack: :retry} = e, metadata) do
    Logger.warn(mod.message(e), metadata)
  end

  defp log_failure_for_exception(%mod{} = e, metadata) do
    Logger.error(mod.message(e), metadata)
  end

  defp maybe_requeue(
         %{
           acknowledger:
             {_, _, %{config: config, queue: queue_name, on_failure: :requeue}}
         } = message
       ) do
    updated_message = %{message | status: :ok}
    config.queue.enqueue(queue_name, updated_message)
    updated_message
  end

  defp maybe_requeue(message), do: message

  defp send_telemetry_event(
         %{
           acknowledger: {_, _, %{config: config}},
           status: status,
           metadata: metadata
         } = message
       ) do
    Telemetry.acknowledger_status(config, status, metadata)

    message
  end
end
