using Akka;
using Akka.Streams;
using Akka.Streams.Stage;
using StackExchange.Redis;

namespace AkkaStreamTweets.Redis;

public class RedisPubSubSource : GraphStageWithMaterializedValue<SourceShape<string>, Task>
{
    private readonly ConnectionMultiplexer _redis;
    private readonly string _channel;

    public RedisPubSubSource(ConnectionMultiplexer redis, string channel)
    {
        _redis = redis;
        _channel = channel;
        Shape = new SourceShape<string>(Out);
    }

    public Outlet<string> Out { get; } = new("RedisPubSubSink.Out");

    public override SourceShape<string> Shape { get; }

    public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
    {
        var completion = new TaskCompletionSource<NotUsed>();
        return new LogicAndMaterializedValue<Task>(new Logic(_redis, _channel, inheritedAttributes, this), completion.Task);
    }

    private sealed class Logic : GraphStageLogic
    {
        private readonly Queue<string> _buffer = new();
        private readonly string _channel;
        private readonly RedisPubSubSource _source;
        private readonly ISubscriber _subscriber;
        private Attributes _inheritedAttributes;

        public Logic(IConnectionMultiplexer redis, string channel, Attributes inheritedAttributes, RedisPubSubSource source) : base(source.Shape)
        {
            _subscriber = redis.GetSubscriber();
            _channel = channel;
            _inheritedAttributes = inheritedAttributes;
            _source = source;

            SetHandler(source.Out, () => { });
        }

        public override void PreStart()
        {
            var callback = GetAsyncCallback<(RedisChannel channel, string bs)>(data =>
            {
                _buffer.Enqueue(data.bs);

                Deliver();
            });

            _subscriber.Subscribe(_channel, (channel, value) => { callback.Invoke((channel, value)); });
        }

        private void Deliver()
        {
            var elem = _buffer.Dequeue();
            Push(_source.Out, elem);
        }
    }
}