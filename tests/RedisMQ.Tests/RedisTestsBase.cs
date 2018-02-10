using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace RedisMQ.Tests
{
    public class RedisTestsBase
    {
        protected readonly string ConnectionString;

        public RedisTestsBase()
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
            string deadLetterQueue = "deadletter")
        {
            var consumeManager = new RedisMessagesConsumerManager(
                NullLogger<RedisMessagesConsumerManager>.Instance,
                ConnectionString,
                new RedisMQSettings
                {
                    TasksQueueName = taskQueue,
                    DeadLetterQueue = deadLetterQueue,
                    InstancesCount = 1,
                    LookupDelaySeconds = 1,
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