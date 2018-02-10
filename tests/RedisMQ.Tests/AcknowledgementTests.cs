using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using Xunit;

namespace RedisMQ.Tests
{
    [Trait("Category", "Acknowledgement")]
    public class AcknowledgementTests : RedisTestsBase
    {
        [Fact(DisplayName = "Acked message should be received exactly once")]
        public async Task ReceiveExactlyOnceAckTest()
        {
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();
            var testDtoHandler = new TestDtoHandler(async ack => await ack.AckAsync());

            using (var manager = CreaterConsumerManager(taskQueue, processingQueuePrefix, new[] {testDtoHandler}))
            {
                manager.Start();
                
                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();
                
                await SendMessage(taskQueue, messageId, testMessage);

                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () => Assert.Equal(1, testDtoHandler.Received.Count),
                    waitIntervalMs: 50,
                    waitRetries: 20);
                
                Assert.Collection(testDtoHandler.Received, (tuple) => Assert.Equal(messageId, tuple.MessageId));
            }
        }

        [Fact(DisplayName = "Should move nacked message to dead letter queue")]
        public async Task NackedToDeadLetterQueueTest()
        {
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();
            var deadLetterQueue = Guid.NewGuid().ToString();

            var testDtoHandler = new TestDtoHandler(async ack => await ack.NackAsync());

            using (var manager = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler},
                deadLetterQueue))
            {
                manager.Start();

                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();

                await SendMessage(taskQueue, messageId, testMessage);

                var redisConnection = await ConnectionMultiplexer.ConnectAsync(ConnectionString);
                var db = redisConnection.GetDatabase();
                
                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: async () =>
                    {
                        var fetched = await db.ListLeftPopAsync(deadLetterQueue);

                        Assert.True(fetched.HasValue);
                        
                        var keybuilder = new DefaultRedisMessageKeyBuilder();
                        var properties = keybuilder.GetMessageProperties(fetched);
                        Assert.Equal(messageId, properties.MessageId);
                    },
                    waitIntervalMs: 50,
                    waitRetries: 20);
            }
        }
        
        [Fact(DisplayName = "Nacked message should be received exactly once")]
        public async Task ReceiveExactlyOnceNackedTest()
        {
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();
            var testDtoHandler = new TestDtoHandler(async ack => await ack.NackAsync());

            using (var manager = CreaterConsumerManager(taskQueue, processingQueuePrefix, new[] {testDtoHandler}))
            {
                manager.Start();
                
                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();
                
                await SendMessage(taskQueue, messageId, testMessage);

                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () => Assert.Equal(1, testDtoHandler.Received.Count),
                    waitIntervalMs: 50,
                    waitRetries: 20);
                
                Assert.Collection(testDtoHandler.Received, (tuple) => Assert.Equal(messageId, tuple.MessageId));
            }
        }
        
        [Theory(DisplayName = "Requeued message should be received exact number of times")]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        public async Task ReceiveExactCountRequeueTest(int requeueCount)
        {
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();

            var requeueLeft = requeueCount;
            var testDtoHandler = new TestDtoHandler(async ack =>
            {
                if (Interlocked.Decrement(ref requeueLeft) > 0)
                {
                    await ack.RequeueAsync();
                }
                else
                {
                    await ack.AckAsync();
                }
            });

            using (var manager = CreaterConsumerManager(taskQueue, processingQueuePrefix, new[] {testDtoHandler}))
            {
                manager.Start();
                
                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();
                
                await SendMessage(taskQueue, messageId, testMessage);

                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () => Assert.Equal(requeueCount, testDtoHandler.Received.Count),
                    waitIntervalMs: 50,
                    waitRetries: 20);
                
                Assert.All(testDtoHandler.Received, (tuple) => Assert.Equal(messageId, tuple.MessageId));
            }
        }
    }
}