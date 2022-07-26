using System.Text;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using AkkaStreamTweets.Redis;
using StackExchange.Redis;
using Tweetinvi;
using Tweetinvi.Models.V2;

namespace AkkaStreamTweets;

internal static class Program
{
    private const string ConsumerKey = "ZolfAquQwGtbNNPI4wuha1cBV";
    private const string ConsumerSecret = "INAa9Q3j210PdnNTjoHpAa1MBHeZ9lJvaMSeanIMTlufwEDzZY";
    private const string BearerToken = "AAAAAAAAAAAAAAAAAAAAAMKVewEAAAAAVMZp1EKUVLQI8Q9WvnEX7qrCvHY%3D4X43OCmPIjgE08uemfama4eZNSJVYF5oc7mEPdC2d5f21ti1g9";

    private static async Task Main(string[] args)
    {
        var tweetSource = Source.ActorRef<TweetV2>(100, OverflowStrategy.DropHead);
        var formatFlow = Flow.Create<TweetV2>().Select(FormatTweet);

        var config = new ConfigurationOptions
        {
            KeepAlive = 0,
            AllowAdmin = true,
            EndPoints = { { "127.0.0.1", 6379 }, { "127.0.0.2", 6379 } },
            ConnectTimeout = 5000,
            ConnectRetry = 5,
            SyncTimeout = 5000,
            AbortOnConnectFail = false,
            Password = "mypassword"
        };
        var connection = await ConnectionMultiplexer.ConnectAsync(config);

        var writeToRedisSink = Sink.FromGraph(new RedisPubSubSink(connection, "redis-pub-sub"));
        var readFromRedisSource = Source.FromGraph(new RedisPubSubSource(connection, "redis-pub-sub"));

        using var sys = ActorSystem.Create("Reactive-Tweets");
        using var mat = sys.Materializer();

        // read from redis again
        _ = readFromRedisSource.To(Sink.ForEach<string>(Console.WriteLine)).Run(mat);

        // read from twitter and write to redis pubsub
        var actor = tweetSource.Via(formatFlow).To(writeToRedisSink).Run(mat);

        // Start Twitter stream
        var userClient = new TwitterClient(ConsumerKey, ConsumerSecret, BearerToken);

        var stream = userClient.StreamsV2.CreateSampleStream();
        stream.TweetReceived += (_, arg) =>
        {
            //Console.WriteLine($"Tweet: {arg.Tweet.AuthorId}{Environment.NewLine}");
            actor.Tell(arg.Tweet);
        }; // push the tweets into the stream
        await stream.StartAsync();

        Console.ReadLine();
    }

    private static string FormatTweet(TweetV2 tweet)
    {
        var builder = new StringBuilder();
        builder.AppendLine("---------------------------------------------------------");
        builder.AppendLine($"Tweet Author ID: {tweet.AuthorId}:");
        //builder.AppendLine();
        //builder.AppendLine(tweet.Text);
        builder.AppendLine("---------------------------------------------------------");
        builder.AppendLine();

        return builder.ToString();
    }
}