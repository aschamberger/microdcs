using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Globalization;
using System.Threading.Channels;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace MicroDCS.Transport.Nats;

/// <summary>
/// The NATS transport (D-37, D-38, D-47). Requests (events, commands, data, metadata) travel through JetStream: a stream per binding prefix,
/// a durable consumer per binding with explicit acknowledgement after the pipeline. Responses use the native inbox pattern: a plain
/// subject per instance (D-9). State topics (<see cref="IStateTopicPort"/>) are keys in a key-value bucket with a per-message time to live.
/// The client library reconnects and resumes consumers by itself (D-10), so the transport has no reconnect loop of its own.
/// </summary>
public sealed class NatsTransport : ITransport, IStateTopicPort, IAsyncDisposable
{
    private const string KvOperationHeader = "KV-Operation";
    private const string KvDeleteOperation = "DEL";

    private static readonly TimeSpan _deleteMarkerTtl = TimeSpan.FromHours(1);

    private static readonly NatsAddressRenderer _renderer = new();
    private static readonly INatsSerialize<ReadOnlyMemory<byte>> _outSerializer = NatsRawSerializer<ReadOnlyMemory<byte>>.Default;
    private static readonly INatsDeserialize<byte[]> _inSerializer = NatsRawSerializer<byte[]>.Default;

    private readonly NatsOptions _options;
    private readonly MicroDcsOptions _core;
    private readonly ProcessorRegistry _registry;
    private readonly IEnvelopeCodec _codec;
    private readonly TimeProvider _time;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<NatsTransport> _logger;
    private readonly ILogger _security;
    private readonly CancellationTokenSource _stopping = new();
    private readonly CancellationTokenSource _receivingStop = new();
    private CancellationTokenSource _consumeCycle = new();
    private readonly ConcurrentDictionary<string, byte> _retainWarned = new(StringComparer.Ordinal);
    private readonly string _clientName;

    private NatsConnection? _connection;
    private NatsJSContext? _js;
    private IInboundHandler? _handler;
    private Channel<NatsDelivery>? _inbox;
    private Task[] _workers = [];
    private Task[] _readers = [];
    private ImmutableArray<NatsConsumerInfo> _consumers = [];
    private readonly ConcurrentDictionary<NatsDelivery, byte> _outstanding = new(ReferenceEqualityComparer.Instance);
    private ReplyAddressRules? _replyRules;
    private Task? _progress;
    private volatile bool _stopped;
    private volatile bool _receiving;
    private int _opened;
    private long _received;
    private long _acknowledged;
    private long _nacked;
    private long _poisoned;
    private long _dropped;
    private long _published;
    private long _reconnects;

    /// <summary>Initializes a new instance of the <see cref="NatsTransport"/> class.</summary>
    /// <param name="options">The NATS options.</param>
    /// <param name="core">The core options (bindings, instance id).</param>
    /// <param name="registry">The processors, for the subscribed intents.</param>
    /// <param name="codec">The envelope codec.</param>
    /// <param name="time">The clock.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public NatsTransport(
        IOptions<NatsOptions> options,
        IOptions<MicroDcsOptions> core,
        ProcessorRegistry registry,
        IEnvelopeCodec codec,
        TimeProvider time,
        ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(core);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _options = options.Value;
        _core = core.Value;
        _registry = registry;
        _codec = codec;
        _time = time;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<NatsTransport>();
        _security = loggerFactory.CreateLogger(PipelineTelemetry.SecurityCategory);
        _clientName = string.IsNullOrWhiteSpace(_options.ClientName) ? "microdcs-" + _core.InstanceId : _options.ClientName;
        Profile = new DeliveryProfile(
            DeliveryGuarantee.AtLeastOnce,
            NativeReply: true,
            NativeExpiry: true,
            StateTopics: true,
            SharedConsumption: true,
            CanRedeliver: true);
    }

    /// <inheritdoc />
    public string Name => NatsOptions.TransportName;

    /// <inheritdoc />
    public DeliveryProfile Profile { get; }

    /// <inheritdoc />
    public IAddressRenderer Addresses => _renderer;

    /// <inheritdoc />
    public bool IsConnected => !_stopped && _connection?.ConnectionState == NatsConnectionState.Open;

    internal NatsStatus Status => new(
        _options.Url,
        _clientName,
        IsConnected,
        _receiving,
        Interlocked.Read(ref _received),
        Interlocked.Read(ref _acknowledged),
        Interlocked.Read(ref _nacked),
        Interlocked.Read(ref _poisoned),
        Interlocked.Read(ref _dropped),
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _reconnects),
        _consumers);

    /// <inheritdoc />
    public ReplyAddress? GetReplyAddress(string bindingId, string instanceId)
    {
        ArgumentNullException.ThrowIfNull(bindingId);
        ArgumentNullException.ThrowIfNull(instanceId);
        return _core.Bindings.TryGetValue(bindingId, out var binding) && !string.IsNullOrEmpty(binding.ReplyBase)
            ? new ReplyAddress(NatsAddressRenderer.NormalizePrefix(binding.ReplyBase) + "." + NatsAddressRenderer.InstanceToken(instanceId))
            : null;
    }

    /// <inheritdoc />
    public async ValueTask StartAsync(IInboundHandler handler, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _handler = handler;
        var plan = BuildPlan();
        _replyRules = BuildReplyRules();

        var connection = new NatsConnection(NatsClientOptionsFactory.Build(_options, _clientName, _loggerFactory, _logger));
        _connection = connection;
        connection.ConnectionOpened += OnOpenedAsync;
        connection.ConnectionDisconnected += OnDisconnectedAsync;

        // A server that is not reachable at start ends the process (D-27); only later losses are retried by the client.
        await connection.ConnectAsync().AsTask().WaitAsync(_options.OperationTimeout, _time, cancellationToken).ConfigureAwait(false);
        Log.Connected(_logger, _options.Url, _clientName);

        var js = new NatsJSContext(connection, new NatsJSOpts(connection.Opts, domain: _options.JetStreamDomain));
        _js = js;
        var kv = new NatsKVContext(js);
        await kv.CreateOrUpdateStoreAsync(
            new NatsKVConfig(_options.StateBucket)
            {
                History = 1,
                NumberOfReplicas = _options.Replicas,
                LimitMarkerTTL = TimeSpan.FromSeconds(1),
            },
            cancellationToken).ConfigureAwait(false);

        foreach (var stream in plan.Streams)
        {
            await js.CreateOrUpdateStreamAsync(
                new StreamConfig(stream.Name, [stream.Subject])
                {
                    MaxAge = _options.StreamMaxAge,
                    NumReplicas = _options.Replicas,
                    AllowMsgTTL = true,
                    Retention = StreamConfigRetention.Limits,
                    DuplicateWindow = TimeSpan.FromMinutes(2),
                },
                cancellationToken).ConfigureAwait(false);
        }

        _inbox = Channel.CreateBounded<NatsDelivery>(new BoundedChannelOptions(Math.Max(1, _core.MessageWorkers) * 2)
        {
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
        _workers = [.. Enumerable.Range(0, Math.Max(1, _core.MessageWorkers)).Select(_ => Task.Run(() => WorkAsync(_inbox.Reader), CancellationToken.None))];

        var readers = new List<Task>();
        var consumers = new List<NatsConsumerInfo>();
        foreach (var consumer in plan.Consumers)
        {
            var config = ConsumerConfigOf(consumer);
            var created = await js.CreateOrUpdateConsumerAsync(consumer.Stream, config, cancellationToken).ConfigureAwait(false);
            var info = new NatsConsumerInfo(consumer.Stream, consumer.Name, consumer.BindingId, InboundKind.Request, string.Join(' ', consumer.Filters));
            consumers.Add(info);
            Log.ConsumerReady(_logger, info.Consumer, info.Stream, info.BindingId, info.Kind, info.Filters);
            readers.Add(Task.Run(() => ReadConsumerAsync(created, info, config), CancellationToken.None));
        }

        foreach (var reply in plan.Replies)
        {
            var subscription = await connection.SubscribeCoreAsync(reply.Subject, serializer: _inSerializer, cancellationToken: cancellationToken).ConfigureAwait(false);
            consumers.Add(new NatsConsumerInfo(string.Empty, reply.Subject, reply.BindingId, InboundKind.Response, reply.Subject));
            Log.ReplySubscribed(_logger, reply.Subject, reply.BindingId);
            readers.Add(Task.Run(() => ReadReplyAsync(subscription, reply.BindingId), CancellationToken.None));
        }

        _consumers = [.. consumers];
        _readers = [.. readers];
        _progress = Task.Run(KeepInProgressAsync, CancellationToken.None);
        _receiving = true;
    }

    /// <inheritdoc />
    public async ValueTask StopReceivingAsync(CancellationToken cancellationToken)
    {
        _receiving = false;
        await _receivingStop.CancelAsync().ConfigureAwait(false);
        try
        {
            await Task.WhenAll(_readers).WaitAsync(cancellationToken).ConfigureAwait(false);

            // In-flight messages finish (D-37 phase 2); the workers end once the inbox is empty.
            _inbox?.Writer.TryComplete();
            await Task.WhenAll(_workers).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The grace period ended; StopAsync cancels what is left.
            await _stopping.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(OutboundMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.Retain && _retainWarned.TryAdd(message.BindingId, 0))
        {
            Log.RetainIgnored(_logger, message.BindingId);
        }

        if (message.SendTo is { } reply)
        {
            // A response goes to the inbox of the requesting instance as a plain message (D-9).
            var (replyHeaders, replyData) = NatsMessageMapper.ToNats(_codec, message, reply.Value, durable: false);
            await PublishCoreAsync(reply.Value, replyData, replyHeaders, cancellationToken).ConfigureAwait(false);
            return;
        }

        var subject = message.Address is { } address
            ? _renderer.RenderPublish(address)
            : throw new ArgumentException("An outbound message needs an address or a reply address.", nameof(message));
        var (headers, data) = NatsMessageMapper.ToNats(_codec, message, subject, durable: true);
        await PublishStreamAsync(subject, data, headers, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(LogicalAddress address, string name, ReadOnlyMemory<byte> payload, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeToLive, TimeSpan.Zero);

        // A key-value bucket is a stream: a put is a publish to $KV.{bucket}.{key}, so the per-message time to live applies (Nats-TTL).
        var headers = new NatsHeaders { [NatsMessageMapper.TtlHeader] = NatsMessageMapper.ToSeconds(timeToLive).ToString(CultureInfo.InvariantCulture) };
        await PublishStreamAsync("$KV." + _options.StateBucket + "." + StateKey(address, name), payload, headers, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DeleteAsync(LogicalAddress address, string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);

        // Written like the client's own delete, with a time to live: without one every removed key would leave a marker for ever (D-51).
        var headers = new NatsHeaders
        {
            [KvOperationHeader] = KvDeleteOperation,
            [NatsMessageMapper.TtlHeader] = NatsMessageMapper.ToSeconds(_deleteMarkerTtl).ToString(CultureInfo.InvariantCulture),
        };
        await PublishStreamAsync("$KV." + _options.StateBucket + "." + StateKey(address, name), ReadOnlyMemory<byte>.Empty, headers, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        _receiving = false;
        await _stopping.CancelAsync().ConfigureAwait(false);
        if (_progress is { } progress)
        {
            await progress.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_connection is { } connection)
        {
            try
            {
                await connection.DisposeAsync().AsTask().WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The grace period ended; the connection closes with the process.
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _stopped = true;
        _stopping.Dispose();
        _receivingStop.Dispose();
        _consumeCycle.Dispose();
        if (_connection is { } connection)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static string StateKey(LogicalAddress address, string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (!name.Split('/').All(LogicalAddress.IsValidSegment))
        {
            throw new ArgumentException("A state topic name may only use segments of letters, digits, '_' and '-'.", nameof(name));
        }

        return _renderer.RenderPublish(address) + "." + name.Replace('/', '.');
    }

    private NatsPlan BuildPlan()
    {
        var streams = new Dictionary<string, NatsStreamPlan>(StringComparer.Ordinal);
        var consumers = new List<NatsConsumerPlan>();
        var replies = new List<NatsReplyPlan>();
        var instance = NatsAddressRenderer.InstanceToken(_core.InstanceId ?? string.Empty);
        foreach (var processor in _core.Roles.HasFlag(InstanceRoles.Processor) ? _registry.Processors : [])
        {
            var binding = _core.Bindings[processor.BindingId];
            if (!string.Equals(binding.Transport, Name, StringComparison.Ordinal))
            {
                continue;
            }

            var prefix = NatsAddressRenderer.NormalizePrefix(binding.Prefix!);
            var streamName = _options.StreamPrefix + "_" + prefix.Replace('.', '_');
            streams.TryAdd(streamName, new NatsStreamPlan(streamName, prefix + ".>"));

            var filters = processor.SubscribeIntents
                .SelectMany(intent => _renderer.RenderSubscriptions(new AddressPattern(binding.Prefix!, binding.ScopeLevels, binding.Discriminator, intent, binding.SharedGroup)))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            if (filters.Count > 0)
            {
                // A shared group is one durable consumer that the instances compete on; without a group each instance has its own and sees every message (D-10).
                // The shared name carries a hash of the configuration: instances that filter differently (a rollout) get their own consumer
                // instead of overwriting each other's (RV-19, D-47 revised).
                var name = binding.SharedGroup is { } group
                    ? NatsAddressRenderer.InstanceToken(processor.BindingId + "-" + group) + "-" + ConfigHash(filters)
                    : NatsAddressRenderer.InstanceToken(processor.BindingId + "-" + instance);
                consumers.Add(new NatsConsumerPlan(streamName, name, processor.BindingId, filters));
            }

            if (GetReplyAddress(processor.BindingId, _core.InstanceId ?? string.Empty) is { } reply)
            {
                // The inbox is exclusive to this instance: a plain subject, no queue group (D-9).
                replies.Add(new NatsReplyPlan(reply.Value, processor.BindingId));
            }
            else if (!processor.PublishIntents.IsDefaultOrEmpty && processor.PublishIntents.Contains(Intent.Command))
            {
                throw new InvalidOperationException($"Binding '{processor.BindingId}' publishes commands over NATS and needs MicroDcs:Bindings:{processor.BindingId}:ReplyBase (D-9).");
            }
        }

        return new NatsPlan([.. streams.Values], consumers, replies);
    }

    private async Task ReadConsumerAsync(INatsJSConsumer consumer, NatsConsumerInfo info, ConsumerConfig config)
    {
        var token = _receivingStop.Token;
        while (!token.IsCancellationRequested)
        {
            // Set when the server stops answering the pull requests of this consumer; ends the cycle like a reconnect does.
            using var gone = new CancellationTokenSource();
            var reason = string.Empty;

            // A reconnect ends the cycle: the pull request of the old connection is gone and the library would notice only after its idle heartbeat timeout.
            using var cycle = CancellationTokenSource.CreateLinkedTokenSource(token, Volatile.Read(ref _consumeCycle).Token, gone.Token);
            var options = new NatsJSConsumeOpts
            {
                // One message at a time from the server (RV-20): the rest waits on the server, where AckWait has not started.
                MaxMsgs = 1,

                // A consumer deleted while a pull request is open answers "409 Consumer Deleted", and the library throws. Deleted while none
                // is open (between pulls, or while disconnected), it does not answer at all: the library reports "no responders" and missed
                // heartbeats as notifications and never throws, so the binding would stall until a restart (RV-14).
                NotificationHandler = (notification, _) =>
                {
                    if (notification is NatsJSNoRespondersNotification or NatsJSTimeoutNotification)
                    {
                        reason = notification.Name;

                        // On the thread pool: the cancellation ends the consume loop that is calling this handler.
                        ThreadPool.QueueUserWorkItem(static source => CancelQuietly(source), gone, preferLocal: false);
                    }

                    return Task.CompletedTask;
                },
            };

            try
            {
                await foreach (var message in consumer.ConsumeAsync(_inSerializer, options, cycle.Token).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref _received);
                    var delivery = new NatsDelivery(info.BindingId, InboundKind.Request, message.Subject, message.Headers, message.Data, (long)(message.Metadata?.NumDelivered ?? 1), message, info.Consumer);

                    // From here until it is settled the message is kept in progress, also while it waits for a worker.
                    _outstanding[delivery] = 0;
                    await _inbox!.Writer.WriteAsync(delivery, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Receiving stopped.
            }
            catch (OperationCanceledException)
            {
                // Reconnected, or the consumer stopped answering (below): pull again.
            }
            catch (ChannelClosedException)
            {
                return;
            }
            catch (Exception ex) when (ex is NatsException or InvalidOperationException)
            {
                // The consumer vanished (server restart without persistence, inactive threshold): create it again and go on (RV-14).
                Log.ConsumeFailed(_logger, ex, info.Consumer);
                consumer = await RecreateConsumerAsync(consumer, info, config, token).ConfigureAwait(false);
                continue;
            }

            // The library ends the enumeration quietly when the cycle is cancelled, so this is checked here and not in a catch.
            if (gone.IsCancellationRequested && !token.IsCancellationRequested)
            {
                Log.ConsumerNotAnswering(_logger, info.Consumer, reason);
                consumer = await RecreateConsumerAsync(consumer, info, config, token).ConfigureAwait(false);
            }
        }
    }

    private static void CancelQuietly(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // A notification after the cycle ended.
        }
    }

    /// <summary>Creates the consumer again after a second; on failure keeps the old handle, and the next round tries again.</summary>
    private async Task<INatsJSConsumer> RecreateConsumerAsync(INatsJSConsumer consumer, NatsConsumerInfo info, ConsumerConfig config, CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1), _time, token).ConfigureAwait(false);
            return await _js!.CreateOrUpdateConsumerAsync(info.Stream, config, token).ConfigureAwait(false);
        }
        catch (Exception inner) when (inner is OperationCanceledException or NatsException)
        {
            // Try again in the next round, unless receiving stopped.
            return consumer;
        }
    }

    private async Task ReadReplyAsync(INatsSub<byte[]> subscription, string bindingId)
    {
        var token = _receivingStop.Token;
        try
        {
            await foreach (var message in subscription.Msgs.ReadAllAsync(token).ConfigureAwait(false))
            {
                Interlocked.Increment(ref _received);
                await _inbox!.Writer.WriteAsync(new NatsDelivery(bindingId, InboundKind.Response, message.Subject, message.Headers, message.Data, 1, null, subscription.Subject), token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Receiving stopped.
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task WorkAsync(ChannelReader<NatsDelivery> reader)
    {
        await foreach (var delivery in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
        {
            try
            {
                var disposition = await ProcessAsync(delivery).ConfigureAwait(false);
                await SettleAsync(delivery, disposition).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The pipeline reports handler faults as an outcome; anything else is a bug in the transport. Do not acknowledge: the server redelivers.
                // A worker never ends while the inbox is open, also not on a cancellation (RV-13).
                if (!_stopping.IsCancellationRequested)
                {
                    Log.WorkerFaulted(_logger, ex, delivery.Subject);
                }
            }
            finally
            {
                _outstanding.TryRemove(delivery, out _);
            }
        }
    }

    private async Task<Disposition> ProcessAsync(NatsDelivery delivery)
    {
        var size = delivery.Data?.Length ?? 0;
        var binding = _core.Bindings[delivery.BindingId];
        if (!_codec.TryDecode(NatsMessageMapper.ToCarrier(delivery.Headers, delivery.Data), out var decoded, out var failure))
        {
            return DropBeforePipeline(delivery.Subject, size, "undecodable: " + failure!.Kind);
        }

        LogicalAddress? address = null;
        if (delivery.Kind == InboundKind.Request && !NatsAddressRenderer.TryParse(binding.Prefix!, binding.Discriminator, delivery.Subject, out address!))
        {
            // A subject that matched the filter but has an invalid scope segment must not skip the scope check (R-11).
            Log.InvalidSubjectRejected(_security, delivery.BindingId);
            return DropBeforePipeline(delivery.Subject, size, "invalid subject segment");
        }

        var replyTo = NatsMessageMapper.GetReplyTo(delivery.Headers);
        if (replyTo is { } reply && ReplyAddressPolicy.Check(reply.Value, _replyRules!, binding) is { } refused)
        {
            // D-48: the instance would publish to the peer's subject with its own permissions ($JS.API, $KV, own subjects).
            Log.ReplyAddressRejected(_security, delivery.BindingId, refused);
            return DropBeforePipeline(delivery.Subject, size, "reply address rejected");
        }

        var inbound = new InboundMessage
        {
            BindingId = delivery.BindingId,
            Kind = delivery.Kind,
            Envelope = NatsMessageMapper.WithNatsExpiry(decoded, delivery.Headers),
            Address = address,
            ReplyTo = replyTo,
            Redelivered = delivery.NumDelivered > 1,
            Subscription = delivery.Subscription,
        };
        var outcome = await _handler!.HandleAsync(inbound, _stopping.Token).ConfigureAwait(false);
        return outcome switch
        {
            DeliveryOutcome.Fault => Disposition.Nak,
            DeliveryOutcome.Drop => Disposition.Terminate,
            _ => Disposition.Ack,
        };
    }

    private Disposition DropBeforePipeline(string subject, int size, string reason)
    {
        Interlocked.Increment(ref _dropped);
        NatsTelemetry.Dropped.Add(1);
        Log.Dropped(_logger, subject, size, reason);
        return Disposition.Terminate;
    }

    private async Task SettleAsync(NatsDelivery delivery, Disposition disposition)
    {
        if (delivery.Message is not { } message)
        {
            // A plain message (response) has nothing to acknowledge.
            return;
        }

        try
        {
            switch (disposition)
            {
                case Disposition.Ack:
                    await message.AckAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    Interlocked.Increment(ref _acknowledged);
                    break;
                case Disposition.Terminate:
                    await message.AckTerminateAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    Interlocked.Increment(ref _acknowledged);
                    break;
                default:
                    if (delivery.NumDelivered >= _options.MaxDeliver)
                    {
                        // The last attempt failed: the server would drop it silently, so end it here where it is counted and logged.
                        Interlocked.Increment(ref _poisoned);
                        NatsTelemetry.DeadLettered.Add(1);
                        Log.Poisoned(_logger, delivery.Subject, delivery.NumDelivered);
                        await message.AckTerminateAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    }
                    else
                    {
                        Interlocked.Increment(ref _nacked);
                        Log.Nacked(_logger, delivery.Subject, delivery.NumDelivered, _options.MaxDeliver);
                        await message.NakAsync(new AckOpts { NakDelay = _options.NakDelay }, CancellationToken.None).ConfigureAwait(false);
                    }

                    break;
            }
        }
        catch (Exception ex) when (ex is NatsException or InvalidOperationException)
        {
            // The connection dropped before the ack: the server redelivers after AckWait and the dedupe store answers the duplicate.
            Log.AcknowledgeFailed(_logger, ex, delivery.Subject);
        }
    }

    private async ValueTask PublishCoreAsync(string subject, ReadOnlyMemory<byte> data, NatsHeaders headers, CancellationToken cancellationToken)
    {
        await WhenConnectedAsync(
            async () => await _connection!.PublishAsync(subject, data, headers, serializer: _outSerializer, cancellationToken: cancellationToken).ConfigureAwait(false),
            subject,
            cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref _published);
        Log.Published(_logger, subject, data.Length, "core");
    }

    private async ValueTask PublishStreamAsync(string subject, ReadOnlyMemory<byte> data, NatsHeaders headers, CancellationToken cancellationToken)
    {
        await WhenConnectedAsync(
            async () =>
            {
                var ack = await _js!.PublishAsync(subject, data, _outSerializer, headers: headers, cancellationToken: cancellationToken).ConfigureAwait(false);
                ack.EnsureSuccess();
            },
            subject,
            cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref _published);
        Log.Published(_logger, subject, data.Length, "jetstream");
    }

    /// <summary>Runs an operation and, while the connection is down, runs it again after a short wait (Python parity: publishing waits for the reconnect). The message id makes a repeated stream publish idempotent.</summary>
    private async ValueTask WhenConnectedAsync(Func<Task> operation, string subject, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (_stopped)
            {
                throw new InvalidOperationException("The transport is stopped.");
            }

            // RV-15: retry when the attempt saw a disconnect, also when the client is back by the time the timeout arrives (a lost
            // PubAck). The epoch counts opened connections; the message id keeps the repeated stream publish idempotent.
            var epoch = Volatile.Read(ref _opened);
            try
            {
                await operation().ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ex is NatsException && !cancellationToken.IsCancellationRequested && !_stopped && (!IsConnected || Volatile.Read(ref _opened) != epoch))
            {
                Log.PublishWaiting(_logger, subject);
                await Task.Delay(TimeSpan.FromMilliseconds(100), _time, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static string ConfigHash(IEnumerable<string> filters) =>
        Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(string.Join('\n', filters.Order(StringComparer.Ordinal)))))[..8];

    private ConsumerConfig ConsumerConfigOf(NatsConsumerPlan consumer) => new(consumer.Name)
    {
        DurableName = consumer.Name,
        AckPolicy = ConsumerConfigAckPolicy.Explicit,
        AckWait = _options.AckWait,
        MaxDeliver = _options.MaxDeliver,
        DeliverPolicy = ConsumerConfigDeliverPolicy.New,
        ReplayPolicy = ConsumerConfigReplayPolicy.Instant,
        FilterSubjects = [.. consumer.Filters],
        InactiveThreshold = _options.ConsumerInactiveThreshold,
        NumReplicas = _options.Replicas,
    };

    private ReplyAddressRules BuildReplyRules()
    {
        var bindings = _core.Bindings.Values.Where(b => string.Equals(b.Transport, Name, StringComparison.Ordinal)).ToList();
        return new ReplyAddressRules(
            '.',
            "*>",
            [.. bindings.Where(static b => !string.IsNullOrEmpty(b.Prefix)).Select(static b => NatsAddressRenderer.NormalizePrefix(b.Prefix!))],
            [.. bindings.Where(static b => !string.IsNullOrEmpty(b.ReplyBase)).Select(static b => NatsAddressRenderer.NormalizePrefix(b.ReplyBase!))]);
    }

    /// <summary>
    /// RV-20: tells the server every <c>AckWait / 3</c> that queued and running messages are still being worked on, so they are not
    /// redelivered while they wait for a worker. A message whose instance dies is redelivered after <c>AckWait</c> as before.
    /// </summary>
    private async Task KeepInProgressAsync()
    {
        var interval = TimeSpan.FromTicks(Math.Max(TimeSpan.FromMilliseconds(100).Ticks, _options.AckWait.Ticks / 3));
        var token = _stopping.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(interval, _time, token).ConfigureAwait(false);
                foreach (var delivery in _outstanding.Keys)
                {
                    if (delivery.Message is { } message)
                    {
                        await SendProgressAsync(delivery, message).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Stopping.
        }
    }

    private async Task SendProgressAsync(NatsDelivery delivery, INatsJSMsg<byte[]> message)
    {
        try
        {
            await message.AckProgressAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is NatsException or InvalidOperationException)
        {
            // The connection is down; the next round tries again, or the server redelivers after AckWait.
            Log.AcknowledgeFailed(_logger, ex, delivery.Subject);
        }
    }

    private ValueTask OnOpenedAsync(object? sender, NatsEventArgs args)
    {
        // The first open is the start; every later one is a reconnect.
        if (Interlocked.Increment(ref _opened) > 1)
        {
            Interlocked.Increment(ref _reconnects);
            NatsTelemetry.Reconnects.Add(1);
            Log.Reconnected(_logger);
            Interlocked.Exchange(ref _consumeCycle, new CancellationTokenSource()).Cancel();
        }

        return ValueTask.CompletedTask;
    }

    private ValueTask OnDisconnectedAsync(object? sender, NatsEventArgs args)
    {
        if (!_stopped)
        {
            Log.ConnectionLost(_logger);
        }

        return ValueTask.CompletedTask;
    }
}

/// <summary>What to do with a delivery once the pipeline is done.</summary>
internal enum Disposition
{
    Ack,
    Terminate,
    Nak,
}

/// <summary>One message taken from a consumer or the reply subscription, on its way to a worker.</summary>
/// <param name="BindingId">The binding.</param>
/// <param name="Kind">Request consumer or the instance's reply subscription.</param>
/// <param name="Subject">The subject the message was published to.</param>
/// <param name="Headers">The headers.</param>
/// <param name="Data">The payload.</param>
/// <param name="NumDelivered">How often the server delivered the message (1 for plain messages).</param>
/// <param name="Message">The JetStream message to acknowledge, or <see langword="null"/> for a plain message.</param>
/// <param name="Subscription">The durable consumer or the reply subscription that received it (telemetry).</param>
internal sealed record NatsDelivery(
    string BindingId,
    InboundKind Kind,
    string Subject,
    NatsHeaders? Headers,
    byte[]? Data,
    long NumDelivered,
    INatsJSMsg<byte[]>? Message,
    string? Subscription = null);

internal sealed record NatsStreamPlan(string Name, string Subject);

internal sealed record NatsConsumerPlan(string Stream, string Name, string BindingId, IReadOnlyList<string> Filters);

internal sealed record NatsReplyPlan(string Subject, string BindingId);

internal sealed record NatsPlan(ImmutableArray<NatsStreamPlan> Streams, IReadOnlyList<NatsConsumerPlan> Consumers, IReadOnlyList<NatsReplyPlan> Replies);

/// <summary>A consumer or reply subscription of the transport, for the inspector.</summary>
/// <param name="Stream">The stream, empty for a reply subscription.</param>
/// <param name="Consumer">The durable consumer name or the reply subject.</param>
/// <param name="BindingId">The binding.</param>
/// <param name="Kind">Request or response.</param>
/// <param name="Filters">The filter subjects.</param>
internal sealed record NatsConsumerInfo(string Stream, string Consumer, string BindingId, InboundKind Kind, string Filters);

/// <summary>A snapshot of the transport for the inspector.</summary>
internal sealed record NatsStatus(
    string Url,
    string ClientName,
    bool Connected,
    bool Receiving,
    long Received,
    long Acknowledged,
    long Nacked,
    long Poisoned,
    long Dropped,
    long Published,
    long Reconnects,
    ImmutableArray<NatsConsumerInfo> Consumers);
