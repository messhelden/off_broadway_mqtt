defmodule OffBroadway.MQTT.Queue do
  @moduledoc """
  Implemens a inmemory queue to buffer incoming messages for a subscription
  from a MQTT broker.
  """
  use GenServer

  alias OffBroadway.MQTT
  alias OffBroadway.MQTT.Config
  alias OffBroadway.MQTT.Telemetry

  @typedoc "Type for queue_names"
  @type name :: OffBroadway.MQTT.queue_name()

  @doc """
  Called by the producer to start a new queue.
  This usually receives a `t:MQTT.queue_name/0` as argument.
  """
  @callback start_link(name) :: GenServer.on_start()

  @doc """
  Called by the producer to dequeue messages.
  """
  @callback enqueue(name, any) :: :ok

  @doc """
  Called by the `Tortoise.Handler` to enqueue incoming messages.
  """
  @callback dequeue(name, non_neg_integer) :: [any]

  @doc """
  Starts a queue with the given name.
  """
  # @spec start_link(args) :: GenServer.on_start()
  # @spec start_link(args) :: {:ok, pid} | :ignore | {:error, any}
  #       when args: nonempty_improper_list(Config.t(), name)
  @spec start_link([name | Config.t() | {atom, any}, ...]) ::
          GenServer.on_start()
  def start_link([%Config{}, {:via, _, _} = queue_name] = args) do
    GenServer.start_link(__MODULE__, args, name: queue_name)
  end

  @impl true
  def init([config, queue_name]) do
    state = %{
      config: config,
      name: queue_name,
      topic_filter: MQTT.topic_from_queue_name(queue_name),
      queue: :queue.new(),
      size: 0
    }

    {:ok, state}
  end

  # defp queue_name({:via, _, _})

  @doc """
  Enqueues the message.
  """
  @spec enqueue(name, any) :: :ok
  def enqueue(queue_name, message) do
    GenServer.call(queue_name, {:enqueue, message})
  end

  @doc """
  Dequeues the demanded amount of messages from the given queue.
  """
  @spec dequeue(name, non_neg_integer) :: [any]
  def dequeue(queue_name, demand) do
    GenServer.call(queue_name, {:dequeue, demand})
  end

  @impl true
  def handle_call(
        {:enqueue, msg},
        _from,
        %{config: config, queue: queue, size: size} = state
      ) do
    updated_queue = :queue.in(msg, queue)
    new_size = size + 1

    Telemetry.queue_in(config, new_size, fn ->
      state_to_telemetry_meta(state)
    end)

    {:reply, :ok, %{state | queue: updated_queue, size: new_size}}
  end

  @impl true
  def handle_call(
        {:dequeue, demand},
        _from,
        %{queue: queue, size: size, config: config} = state
      ) do
    {remaining, messages, taken} = take(queue, demand)
    new_size = size - taken

    Telemetry.queue_out(config, taken, new_size, fn ->
      state_to_telemetry_meta(state)
    end)

    {:reply, messages, %{state | queue: remaining, size: new_size}}
  end

  defp take(queue, amount), do: do_take(queue, amount, :queue.new(), 0)

  defp do_take(queue, amount, acc, size) when amount > 0 do
    case :queue.out(queue) do
      {{:value, value}, updated_queue} ->
        updated_acc = :queue.in(value, acc)
        do_take(updated_queue, amount - 1, updated_acc, size + 1)

      {:empty, updated_queue} ->
        {updated_queue, :queue.to_list(acc), size}
    end
  end

  defp do_take(queue, _amount, acc, size),
    do: {queue, :queue.to_list(acc), size}

  defp state_to_telemetry_meta(state) do
    Map.take(state, [:topic_filter])
  end
end
