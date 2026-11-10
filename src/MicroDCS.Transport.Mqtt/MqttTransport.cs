using System.Buffers;
using System.Collections.Immutable;
using System.Threading.Channels;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Exceptions;
using MQTTnet.Protocol;

namespace MicroDCS.Transport.Mqtt;

/// <summary>
/// The MQTT 5 transport (D-37, D-38). One connection per instance with a persistent session carries the subscriptions of all
/// MQTT bindings, the per-instance reply topic (D-9) and the retained state topics (<see cref="IStateTopicPort"/>). Messages are
/// acknowledged manually after the pipeline finished them, so a crash in between means redelivery, never loss.
/// </summary>
public sealed class MqttTransport : ITransport, IStateTopicPort, IAsyncDisposable
{
    /// <summary>Sent attempts per message before the publish fails (RV-16); waiting for a reconnect does not count.</summary>
    public const int MaxPublishAttempts = 5;

    private static readonly MqttAddressRenderer _renderer = new();

    private readonly MqttOptions _options;
    private readonly MicroDcsOptions _core;
    private readonly ProcessorRegistry _registry;
    private readonly IEnvelopeCodec _codec;
    private readonly TimeProvider _time;
    private readonly ILogger<MqttTransport> _logger;
    private readonly ILogger _security;
    private readonly CancellationTokenSource _stopping = new();
    private readonly string _clientId;

    private IMqttClient? _client;
    private IInboundHandler? _handler;
    private Channel<MqttApplicationMessageReceivedEventArgs>? _inbox;
    private Task[] _workers = [];
    private ImmutableArray<MqttSubscription> _subscriptions = [];
    private ReplyAddressRules? _replyRules;
    private TaskCompletionSource _up = NewSignal();
    private Task? _reconnect;
    private volatile bool _stopped;
    private volatile bool _receiving;
    private int _reconnecting;
    private long _received;
    private long _acknowledged;
    private long _dropped;
    private long _published;
    private long _reconnects;
    private string _lastDisconnect = string.Empty;

    /// <summary>Initializes a new instance of the <see cref="MqttTransport"/> class.</summary>
    /// <param name="options">The MQTT options.</param>
    /// <param name="core">The core options (bindings, instance id).</param>
    /// <param name="registry">The processors, for the subscribed intents.</param>
    /// <param name="codec">The envelope codec.</param>
    /// <param name="time">The clock.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public MqttTransport(
        IOptions<MqttOptions> options,
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
        _logger = loggerFactory.CreateLogger<MqttTransport>();
        _security = loggerFactory.CreateLogger(PipelineTelemetry.SecurityCategory);
        _clientId = string.IsNullOrWhiteSpace(_options.ClientId) ? "microdcs-" + _core.InstanceId : _options.ClientId;
        Profile = new DeliveryProfile(
            _options.PublishQos == 0 ? DeliveryGuarantee.AtMostOnce : DeliveryGuarantee.AtLeastOnce,
            NativeReply: true,
            NativeExpiry: true,
            StateTopics: true,
            SharedConsumption: true,
            CanRedeliver: false);
    }

    /// <inheritdoc />
    public string Name => MqttOptions.TransportName;

    /// <inheritdoc />
    public DeliveryProfile Profile { get; }

    /// <inheritdoc />
    public IAddressRenderer Addresses => _renderer;

    /// <inheritdoc />
    public bool IsConnected => !_stopped && _client?.IsConnected == true;

    internal MqttStatus Status => new(
        _options.Hostname,
        _options.Port,
        _clientId,
        IsConnected,
        _receiving,
        Interlocked.Read(ref _received),
        Interlocked.Read(ref _acknowledged),
        Interlocked.Read(ref _dropped),
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _reconnects),
        Volatile.Read(ref _lastDisconnect),
        _subscriptions);

    /// <inheritdoc />
    public ReplyAddress? GetReplyAddress(string bindingId, string instanceId)
    {
        ArgumentNullException.ThrowIfNull(bindingId);
        ArgumentNullException.ThrowIfNull(instanceId);
        return _core.Bindings.TryGetValue(bindingId, out var binding) && !string.IsNullOrEmpty(binding.ReplyBase)
            ? new ReplyAddress(binding.ReplyBase + "/" + instanceId)
            : null;
    }

    /// <inheritdoc />
    public async ValueTask StartAsync(IInboundHandler handler, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _handler = handler;
        _subscriptions = BuildSubscriptions();
        _replyRules = BuildReplyRules();
        var clientOptions = BuildClientOptions();

        _inbox = Channel.CreateBounded<MqttApplicationMessageReceivedEventArgs>(new BoundedChannelOptions(Math.Max(1, _core.MessageWorkers) * 2)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        _workers = [.. Enumerable.Range(0, Math.Max(1, _core.MessageWorkers)).Select(_ => Task.Run(() => WorkAsync(_inbox.Reader), CancellationToken.None))];

        var client = new MqttClientFactory().CreateMqttClient();
        _client = client;
        client.ApplicationMessageReceivedAsync += OnReceivedAsync;
        client.DisconnectedAsync += OnDisconnectedAsync;

        // A broker that is not reachable at start ends the process (D-27); only later losses are retried.
        var result = await client.ConnectAsync(clientOptions, cancellationToken).ConfigureAwait(false);
        Log.Connected(_logger, _options.Hostname, _options.Port, _clientId, result.IsSessionPresent);
        await SubscribeAsync(client, cancellationToken).ConfigureAwait(false);
        _receiving = true;
        _up.TrySetResult();
    }

    /// <inheritdoc />
    public async ValueTask StopReceivingAsync(CancellationToken cancellationToken)
    {
        _receiving = false;
        var client = _client;
        if (client is { IsConnected: true })
        {
            foreach (var filter in _subscriptions.Select(static s => s.Filter).Distinct(StringComparer.Ordinal))
            {
                try
                {
                    await client.UnsubscribeAsync(filter, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is MqttCommunicationException or OperationCanceledException or InvalidOperationException)
                {
                    Log.UnsubscribeFailed(_logger, ex, filter);
                }
            }
        }

        // In-flight messages finish (D-37 phase 2); the workers end once the inbox is empty.
        _inbox?.Writer.TryComplete();
        try
        {
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
        var topic = message.SendTo?.Value
            ?? (message.Address is { } address
                ? _renderer.RenderPublish(address)
                : throw new ArgumentException("An outbound message needs an address or a reply address.", nameof(message)));
        var mqtt = MqttMessageMapper.ToMqtt(_codec, message, topic, Qos());
        await PublishWhenConnectedAsync(mqtt, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask PublishAsync(LogicalAddress address, string name, ReadOnlyMemory<byte> payload, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeToLive, TimeSpan.Zero);
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(StateTopic(address, name))
            .WithPayload(new ReadOnlySequence<byte>(payload))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithRetainFlag()
            .WithMessageExpiryInterval(MqttMessageMapper.ToSeconds(timeToLive))
            .Build();
        return PublishWhenConnectedAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(LogicalAddress address, string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);

        // A zero-byte retained message clears the topic (Python parity: delete_retained).
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(StateTopic(address, name))
            .WithPayload(ReadOnlySequence<byte>.Empty)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithRetainFlag()
            .Build();
        return PublishWhenConnectedAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        _receiving = false;
        await _stopping.CancelAsync().ConfigureAwait(false);
        _up.TrySetCanceled(CancellationToken.None);
        if (_reconnect is { } reconnect)
        {
            await reconnect.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        var client = _client;
        if (client is { IsConnected: true })
        {
            try
            {
                await client.DisconnectAsync(new MqttClientDisconnectOptions { Reason = MqttClientDisconnectOptionsReason.NormalDisconnection }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is MqttCommunicationException or OperationCanceledException or InvalidOperationException)
            {
                // The broker closes the session on its side; nothing to retry on shutdown.
                Log.ConnectionLost(_logger, ex.GetType().Name);
            }
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _stopped = true;
        _stopping.Dispose();
        _client?.Dispose();
        return ValueTask.CompletedTask;
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static string StateTopic(LogicalAddress address, string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (!name.Split('/').All(LogicalAddress.IsValidSegment))
        {
            throw new ArgumentException("A state topic name may only use segments of letters, digits, '_' and '-'.", nameof(name));
        }

        return _renderer.RenderPublish(address) + "/" + name;
    }

    private MqttQualityOfServiceLevel Qos() =>
        _options.PublishQos == 0 ? MqttQualityOfServiceLevel.AtMostOnce : MqttQualityOfServiceLevel.AtLeastOnce;

    private ImmutableArray<MqttSubscription> BuildSubscriptions()
    {
        var result = new List<MqttSubscription>();
        foreach (var processor in _core.Roles.HasFlag(InstanceRoles.Processor) ? _registry.Processors : [])
        {
            var binding = _core.Bindings[processor.BindingId];
            if (!string.Equals(binding.Transport, Name, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var intent in processor.SubscribeIntents)
            {
                var pattern = new AddressPattern(binding.Prefix!, binding.ScopeLevels, binding.Discriminator, intent, binding.SharedGroup);
                result.AddRange(_renderer.RenderSubscriptions(pattern).Select(f => new MqttSubscription(f, processor.BindingId, InboundKind.Request)));
            }

            if (GetReplyAddress(processor.BindingId, _core.InstanceId ?? string.Empty) is { } reply)
            {
                // The reply topic is exclusive to this instance: never shared (D-9).
                result.Add(new MqttSubscription(reply.Value, processor.BindingId, InboundKind.Response));
            }
            else if (!processor.PublishIntents.IsDefaultOrEmpty && processor.PublishIntents.Contains(Intent.Command))
            {
                throw new InvalidOperationException($"Binding '{processor.BindingId}' publishes commands over MQTT and needs MicroDcs:Bindings:{processor.BindingId}:ReplyBase (D-9).");
            }
        }

        return [.. result];
    }

    private MqttClientOptions BuildClientOptions() => MqttClientOptionsFactory.Build(_options, _clientId, _logger, resumeSession: _core.Roles.HasFlag(InstanceRoles.Processor));

    private async Task SubscribeAsync(IMqttClient client, CancellationToken cancellationToken)
    {
        foreach (var group in _subscriptions.GroupBy(static s => s.Filter, StringComparer.Ordinal))
        {
            var options = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(group.Key, MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();
            var result = await client.SubscribeAsync(options, cancellationToken).ConfigureAwait(false);
            var failed = result.Items.FirstOrDefault(static i => (int)i.ResultCode >= 128);
            if (failed is not null)
            {
                throw new InvalidOperationException($"The broker refused the subscription to '{group.Key}': {failed.ResultCode}.");
            }

            foreach (var subscription in group)
            {
                Log.Subscribed(_logger, subscription.Filter, subscription.BindingId, subscription.Kind);
            }
        }
    }

    private async Task OnReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        args.AutoAcknowledge = false;
        Interlocked.Increment(ref _received);
        var inbox = _inbox;
        try
        {
            if (!_receiving || inbox is null)
            {
                // Not acknowledged: the session redelivers after the next connect (the unsubscribe raced with the delivery).
                return;
            }

            await inbox.Writer.WriteAsync(args, _stopping.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is ChannelClosedException or OperationCanceledException)
        {
            // Shutting down: leave the message unacknowledged.
        }
    }

    private async Task WorkAsync(ChannelReader<MqttApplicationMessageReceivedEventArgs> reader)
    {
        await foreach (var args in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
        {
            var topic = args.ApplicationMessage.Topic;
            try
            {
                var acknowledge = await ProcessAsync(args.ApplicationMessage).ConfigureAwait(false);
                if (acknowledge)
                {
                    await AcknowledgeAsync(args).ConfigureAwait(false);
                }
                else
                {
                    Log.LeftUnacknowledged(_logger, topic);
                }
            }
            catch (Exception ex)
            {
                // The pipeline reports handler faults as an outcome; anything else is a bug in the transport. Do not acknowledge.
                // A worker never ends while the inbox is open, also not on a cancellation (RV-13).
                if (!_stopping.IsCancellationRequested)
                {
                    Log.WorkerFaulted(_logger, ex, topic);
                }
            }
        }
    }

    /// <summary>Decodes and hands a message to the pipeline once per matching binding.</summary>
    /// <returns><see langword="true"/> when the message may be acknowledged.</returns>
    private async Task<bool> ProcessAsync(MqttApplicationMessage message)
    {
        var topic = message.Topic;
        var size = (int)message.Payload.Length;
        var matches = _subscriptions.Where(s => MqttAddressRenderer.Matches(s.Filter, topic)).DistinctBy(static s => (s.BindingId, s.Kind)).ToList();
        if (matches.Count == 0)
        {
            return DropBeforePipeline(topic, size, "no matching subscription");
        }

        if (!_codec.TryDecode(MqttMessageMapper.ToCarrier(message), out var decoded, out var failure))
        {
            return DropBeforePipeline(topic, size, "undecodable: " + failure!.Kind);
        }

        var envelope = MqttMessageMapper.WithMqttExpiry(decoded, message);
        var fault = false;
        foreach (var match in matches)
        {
            var binding = _core.Bindings[match.BindingId];
            LogicalAddress? address = null;
            if (match.Kind == InboundKind.Request && !MqttAddressRenderer.TryParse(binding.Prefix!, binding.Discriminator, topic, out address!))
            {
                // A topic that matched the filter but has an invalid scope segment must not skip the scope check (R-11).
                Log.InvalidTopicRejected(_security, match.BindingId);
                DropBeforePipeline(topic, size, "invalid topic segment");
                continue;
            }

            if (!string.IsNullOrEmpty(message.ResponseTopic) && ReplyAddressPolicy.Check(message.ResponseTopic, _replyRules!, binding) is { } refused)
            {
                // D-48: the instance would publish to the peer's address with its own permissions.
                Log.ReplyAddressRejected(_security, match.BindingId, refused);
                DropBeforePipeline(topic, size, "reply address rejected");
                continue;
            }

            var inbound = new InboundMessage
            {
                BindingId = match.BindingId,
                Kind = match.Kind,
                Envelope = envelope,
                Address = address,
                ReplyTo = string.IsNullOrEmpty(message.ResponseTopic) ? null : new ReplyAddress(message.ResponseTopic),
                Redelivered = message.Dup,
                Subscription = match.Filter,
            };
            var outcome = await _handler!.HandleAsync(inbound, _stopping.Token).ConfigureAwait(false);
            fault |= outcome == DeliveryOutcome.Fault;
        }

        return !fault;
    }

    private bool DropBeforePipeline(string topic, int size, string reason)
    {
        Interlocked.Increment(ref _dropped);
        MqttTelemetry.Dropped.Add(1);
        Log.Dropped(_logger, topic, size, reason);
        return true;
    }

    private async Task AcknowledgeAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        if (args.ApplicationMessage.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce)
        {
            return;
        }

        try
        {
            await args.AcknowledgeAsync(CancellationToken.None).ConfigureAwait(false);
            Interlocked.Increment(ref _acknowledged);
        }
        catch (Exception ex) when (ex is MqttCommunicationException or InvalidOperationException)
        {
            // The connection dropped before the ack: the broker redelivers and the dedupe store answers the duplicate.
            Log.AcknowledgeFailed(_logger, ex, args.ApplicationMessage.Topic);
        }
    }

    private async ValueTask PublishWhenConnectedAsync(MqttApplicationMessage message, CancellationToken cancellationToken)
    {
        var attempts = 0;
        while (true)
        {
            if (_stopped)
            {
                throw new InvalidOperationException("The transport is stopped.");
            }

            await WaitUntilConnectedAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var result = await _client!.PublishAsync(message, cancellationToken).ConfigureAwait(false);
                if (result.ReasonCode is not (MqttClientPublishReasonCode.Success or MqttClientPublishReasonCode.NoMatchingSubscribers))
                {
                    throw new InvalidOperationException($"The broker rejected the publish to '{message.Topic}': {result.ReasonCode}.");
                }

                Interlocked.Increment(ref _published);
                var size = (int)message.Payload.Length;
                var qos = (int)message.QualityOfServiceLevel;
                Log.Published(_logger, message.Topic, size, qos);
                return;
            }
            catch (MqttCommunicationException) when (++attempts < MaxPublishAttempts)
            {
                // The connection dropped under the publish: wait for the reconnect and send again (Python parity). A message that
                // makes the broker drop the connection every time (ACL, packet size) fails after MaxPublishAttempts (RV-16, D-27).
                Log.PublishWaiting(_logger, message.Topic);
                await Task.Delay(TimeSpan.FromMilliseconds(100), _time, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask WaitUntilConnectedAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Volatile.Read(ref _up).Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (TaskCanceledException) when (_stopped && !cancellationToken.IsCancellationRequested)
        {
            throw new InvalidOperationException("The transport is stopped.");
        }
    }

    private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        if (_stopped)
        {
            return Task.CompletedTask;
        }

        var reason = args.Reason.ToString();
        Volatile.Write(ref _lastDisconnect, reason);
        Log.ConnectionLost(_logger, reason);
        if (Volatile.Read(ref _up).Task.IsCompleted)
        {
            Volatile.Write(ref _up, NewSignal());
        }

        EnsureReconnecting();
        return Task.CompletedTask;
    }

    private void EnsureReconnecting()
    {
        if (Interlocked.CompareExchange(ref _reconnecting, 1, 0) == 0)
        {
            _reconnect = Task.Run(ReconnectAsync, CancellationToken.None);
        }
    }

    private async Task ReconnectAsync()
    {
        var delay = _options.ReconnectMinDelay;
        try
        {
            while (!_stopped)
            {
                await Task.Delay(delay, _time, _stopping.Token).ConfigureAwait(false);
                try
                {
                    // Built again for every attempt: a rotated token or password file is read here (RV-17).
                    var result = await _client!.ConnectAsync(BuildClientOptions(), _stopping.Token).ConfigureAwait(false);
                    Log.Connected(_logger, _options.Hostname, _options.Port, _clientId, result.IsSessionPresent);
                    if (_receiving)
                    {
                        // Subscribing again is cheap and also covers a broker that lost the session.
                        await SubscribeAsync(_client, _stopping.Token).ConfigureAwait(false);
                    }

                    Interlocked.Increment(ref _reconnects);
                    MqttTelemetry.Reconnects.Add(1);
                    Volatile.Read(ref _up).TrySetResult();
                    return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    delay = TimeSpan.FromTicks(Math.Min(_options.ReconnectMaxDelay.Ticks, delay.Ticks * 2));
                    Log.ReconnectFailed(_logger, ex, delay);

                    // RV-11: connected but the subscribe failed; a connect on a connected client throws forever, so drop the link first.
                    await DisconnectQuietlyAsync().ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Stopping.
        }
        finally
        {
            Volatile.Write(ref _reconnecting, 0);

            // RV-12: a disconnect between the connect and this reset found the loop still running and started none. Look again.
            if (!_stopped && _client is { IsConnected: false })
            {
                if (Volatile.Read(ref _up).Task.IsCompleted)
                {
                    Volatile.Write(ref _up, NewSignal());
                }

                EnsureReconnecting();
            }
        }
    }

    private async Task DisconnectQuietlyAsync()
    {
        if (_client is not { IsConnected: true } client)
        {
            return;
        }

        try
        {
            await client.DisconnectAsync(new MqttClientDisconnectOptions { Reason = MqttClientDisconnectOptionsReason.NormalDisconnection }, _stopping.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is MqttCommunicationException or OperationCanceledException or InvalidOperationException)
        {
            Log.ConnectionLost(_logger, ex.GetType().Name);
        }
    }

    private ReplyAddressRules BuildReplyRules()
    {
        var bindings = _core.Bindings.Values.Where(b => string.Equals(b.Transport, Name, StringComparison.Ordinal)).ToList();
        return new ReplyAddressRules(
            '/',
            "+#",
            [.. bindings.Where(static b => !string.IsNullOrEmpty(b.Prefix)).Select(static b => b.Prefix!.Trim('/'))],
            [.. bindings.Where(static b => !string.IsNullOrEmpty(b.ReplyBase)).Select(static b => b.ReplyBase!.Trim('/'))]);
    }
}

/// <summary>One subscribed filter and what it belongs to.</summary>
/// <param name="Filter">The topic filter as subscribed, including a <c>$share</c> prefix.</param>
/// <param name="BindingId">The binding.</param>
/// <param name="Kind">Request subscription or the instance's reply subscription.</param>
internal sealed record MqttSubscription(string Filter, string BindingId, InboundKind Kind);

/// <summary>A snapshot of the transport for the inspector.</summary>
internal sealed record MqttStatus(
    string Host,
    int Port,
    string ClientId,
    bool Connected,
    bool Receiving,
    long Received,
    long Acknowledged,
    long Dropped,
    long Published,
    long Reconnects,
    string LastDisconnect,
    ImmutableArray<MqttSubscription> Subscriptions);
