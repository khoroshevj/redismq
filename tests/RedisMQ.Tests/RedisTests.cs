using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Xunit;

namespace RedisMQ.Tests
{
    public class RedisTestsBase
    {
    }
    
    public class RedisTests : RedisTestsBase
    {
        [Fact]
        public async Task AckTest()
        {
            var redisConnection = "127.0.0.1:6379,abortConnect=false";
            var multiplexer = ConnectionMultiplexer.Connect(redisConnection);
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();

            var consumeManager = new RedisMessagesConsumerManager(
                NullLogger<RedisMessagesConsumerManager>.Instance,
                redisConnection,
                new RedisMQSettings
                {
                    TasksQueueName = taskQueue,
                    DeadLetterQueue = "deadletter",
                    InstancesCount = 1,
                    LookupDelaySeconds = 1,
                    ProcessingQueuePrefix = processingQueuePrefix
                });
            
            var sender = new RedisMessageSender(taskQueue);

            try
            {
                var testDtoHandler = new TestDtoHandler(async ack => await ack.AckAsync());

                consumeManager.RegisterMessageHandler(testDtoHandler);
                consumeManager.Start();

                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestDto("Name", 777);
                
                var db = multiplexer.GetDatabase();
                var tr = db.CreateTransaction();
                
                sender.Send(tr, messageId, testMessage);

                await tr.ExecuteAsync();
                
                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () => Assert.True(testDtoHandler.Received.Count == 1),
                    waitIntervalMs: 50,
                    waitRetries: 20);
                
                Assert.Collection(testDtoHandler.Received, (tuple) => Assert.Equal(messageId, tuple.MessageId));
            }
            finally
            {
                consumeManager?.Dispose();
            }
        }
        
        [RedisQueueMessage(payloadTypeName: "test_dto")]
        public class TestDto
        {
            public TestDto(string name, int value)
            {
                Name = name;
                Value = value;
            }

            public string Name { get; set; }
            public int Value { get; set; }
        }

        public class TestDtoHandler : RedisMessageHandlerBase<TestDto>
        {
            private readonly Func<IAcknowledgement, Task> _acknowledgementTask;

            public List<(string MessageId, TestDto Message)> Received { get; }

            public TestDtoHandler(Func<IAcknowledgement, Task> acknowledgementTask) : base(NullLogger.Instance)
            {
                _acknowledgementTask = acknowledgementTask;
                Received = new List<(string, TestDto)>();
            }

            public override async Task HandleMessageAsync(IAcknowledgement acknowledgement, TestDto message, string messageId)
            {
                Received.Add((messageId, message));

                await _acknowledgementTask(acknowledgement);
            }
        }
    }
}