using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace RedisMQ.Tests
{
    [Trait("Category", "Reliability")]
    public class ReliabilityTests : RedisTestsBase
    {
        private readonly ITestOutputHelper _output;

        public ReliabilityTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact(DisplayName = "When one of consumers fails second should start processing messages")]
        public async Task SecondShouldStartProcessingTest()
        {
            var messagesCount = 500;
            var stopConsumerAfterNumberOfMessages = 50;
            var workDurationMs = 5;
            
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();

            var sendMessagesIds = new string[messagesCount];
            for (var i = 0; i < messagesCount; i++)
            {
                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();
                
                await SendMessage(taskQueue, messageId, testMessage);
                sendMessagesIds[i] = messageId;
            }
            
            var testDtoHandler1 = new TestDtoHandler(DelayedAcknowledgementTask);
            var testDtoHandler2 = new TestDtoHandler(AcknowledgementTask);
            
            using (var manager1 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler1},
                instanceCount: 1,
                lookupDelayMilliseconds: 100,
                output: _output))
            using (var manager2 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler2},
                instanceCount: 1,
                lookupDelayMilliseconds: 100,
                output: _output))
            {
                manager1.Start();
                await Task.Delay(10);
                manager2.Start();

                await Task.Delay(workDurationMs * stopConsumerAfterNumberOfMessages);
                
                Assert.Equal(0, testDtoHandler2.Received.Count);
                manager1.Dispose();

                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () =>
                        Assert.Equal(
                            expected: messagesCount,
                            actual: testDtoHandler1.Received.Count + testDtoHandler2.Received.Count),
                    waitIntervalMs: 100,
                    waitRetries: 100);

                // check both handlers received messages
                Assert.NotInRange(testDtoHandler1.Received.Count, 0, 1);
                Assert.NotInRange(testDtoHandler2.Received.Count, 0, 1);
                
                Assert.All(testDtoHandler1.Received,
                    (tuple) => Assert.Contains(sendMessagesIds, id => id == tuple.MessageId));
            }

            async Task DelayedAcknowledgementTask(IAcknowledgement ack)
            {
                await Task.Delay(workDurationMs);
                await ack.AckAsync();
            }

            async Task AcknowledgementTask(IAcknowledgement ack)
            {
                await ack.AckAsync();
            }
        }
        
        [Fact(DisplayName = "All messages should be received by two consumer even if three started")]
        public async Task StartingMoreThanInstanceCountConsumersTest()
        {
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();

            var messagesCount = 500;
            var workDurationMs = 10;

            for (var i = 0; i < messagesCount; i++)
            {
                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();
                
                await SendMessage(taskQueue, messageId, testMessage);
            }
            
            var testDtoHandler1 = new TestDtoHandler(AcknowledgementTask);
            var testDtoHandler2 = new TestDtoHandler(AcknowledgementTask);
            var testDtoHandler3 = new TestDtoHandler(AcknowledgementTask);
            
            using (var manager1 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler1},
                instanceCount: 2,
                lookupDelayMilliseconds: 50))
            using (var manager2 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler2},
                instanceCount: 2,
                lookupDelayMilliseconds: 50))
            using (var manager3 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler3},
                instanceCount: 2,
                lookupDelayMilliseconds: 50))
            {
                manager1.Start();
                manager2.Start();
                
                await Task.Delay(workDurationMs * 2);
                
                manager3.Start();
                
                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () =>
                        Assert.Equal(
                            expected: messagesCount,
                            actual: testDtoHandler1.Received.Count + testDtoHandler2.Received.Count),
                    waitIntervalMs: 50,
                    waitRetries: 200);
                
                Assert.Equal(0, testDtoHandler3.Received.Count);
            }

            async Task AcknowledgementTask(IAcknowledgement ack)
            {
                await Task.Delay(workDurationMs);
                await ack.AckAsync();
            }
        }
        
        [Fact(DisplayName = "All messages should be received even if one of consumers fails")]
        public async Task FailedConsumerMessagesReceivingTest()
        {
            var messagesCount = 500;
            var stopConsumerAfterNumberOfMessages = 50;
            var workDurationMs = 5;
            
            var taskQueue = Guid.NewGuid().ToString();
            var processingQueuePrefix = Guid.NewGuid().ToString();

            var sendMessagesIds = new string[messagesCount];
            for (var i = 0; i < messagesCount; i++)
            {
                var messageId = Guid.NewGuid().ToString();
                var testMessage = new TestMessage();
                
                await SendMessage(taskQueue, messageId, testMessage);
                sendMessagesIds[i] = messageId;
            }
            
            var testDtoHandler1 = new TestDtoHandler(DelayedAcknowledgementTask);
            var testDtoHandler2 = new TestDtoHandler(DelayedAcknowledgementTask);
            
            using (var manager1 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler1},
                instanceCount: 2,
                lookupDelayMilliseconds: 50))
            using (var manager2 = CreaterConsumerManager(
                taskQueue,
                processingQueuePrefix,
                new[] {testDtoHandler2},
                instanceCount: 2,
                lookupDelayMilliseconds: 50))
            {
                manager1.Start();
                manager2.Start();

                await Task.Delay(workDurationMs * stopConsumerAfterNumberOfMessages);

                manager1.Dispose();

                await WaitingAssert.CheckBecomeTrueAsync(
                    assertion: () =>
                        Assert.Equal(
                            expected: messagesCount,
                            actual: testDtoHandler1.Received.Count + testDtoHandler2.Received.Count),
                    waitIntervalMs: 50,
                    waitRetries: 200);

                // check both handlers received messages
                Assert.NotInRange(testDtoHandler1.Received.Count, 0, 1);
                Assert.NotInRange(testDtoHandler2.Received.Count, 0, 1);
                
                Assert.All(testDtoHandler1.Received,
                    (tuple) => Assert.Contains(sendMessagesIds, id => id == tuple.MessageId));
            }

            async Task DelayedAcknowledgementTask(IAcknowledgement ack)
            {
                await Task.Delay(workDurationMs);
                await ack.AckAsync();
            }
        }
    }
}
