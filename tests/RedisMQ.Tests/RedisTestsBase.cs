using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace RedisMQ.Tests
{
    public abstract class RedisTestsBase
    {
        protected readonly string ConnectionString;

        protected RedisTestsBase()
        {
            ConnectionString = "127.0.0.1:6379,abortConnect=false";
        }
        
        protected async Task SendMessage(string taskQueue, string messageId, TestMessage testMessage)
        {
            var multiplexer = ConnectionMultiplexer.Connect(ConnectionString);
            var db = multiplexer.GetDatabase();
            var tr = db.CreateTransaction();

            var sender = new RedisMessageSender(taskQueue);
            sender.Send(tr, messageId, testMessage);

            await tr.ExecuteAsync();
        }

        protected RedisMessagesConsumerManager CreaterConsumerManager(
            string taskQueue,
            string processingQueuePrefix,
            TestDtoHandler[] testDtoHandlers,
            string deadLetterQueue = "deadletter",
            int instanceCount = 1,
            int lookupDelayMilliseconds = 1)
        {
            var consumeManager = new RedisMessagesConsumerManager(
                NullLogger<RedisMessagesConsumerManager>.Instance,
                ConnectionString,
                new RedisMQSettings
                {
                    TasksQueueName = taskQueue,
                    DeadLetterQueue = deadLetterQueue,
                    InstancesCount = instanceCount,
                    LookupDelayMilliseconds = lookupDelayMilliseconds,
                    ProcessingQueuePrefix = processingQueuePrefix
                },
                new DefaultRedisMessageKeyBuilder());

            foreach (var handler in testDtoHandlers)
            {
                consumeManager.RegisterMessageHandler(handler);
            }

            return consumeManager;
        }
    }
}