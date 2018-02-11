using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;

namespace RedisMQ.Tests
{
    [RedisQueueMessage(payloadTypeName: "test_dto")]
    public class TestMessage
    {
        public string Name { get; }
        public string Value { get; }
        
        public TestMessage()
            : this(Guid.NewGuid().ToString(), Guid.NewGuid().ToString())
        {
        }
        
        public TestMessage(string name, string value)
        {
            Name = name;
            Value = value;
        }
    }

    public class TestDtoHandler : RedisMessageHandlerBase<TestMessage>
    {
        private readonly Func<IAcknowledgement, Task> _acknowledgementTask;

        public ConcurrentBag<(string MessageId, TestMessage Message)> Received { get; }

        public TestDtoHandler(Func<IAcknowledgement, Task> acknowledgementTask)
            : base(NullLogger.Instance)
        {
            _acknowledgementTask = acknowledgementTask;
            Received = new ConcurrentBag<(string, TestMessage)>();
        }

        public override async Task HandleMessageAsync(
            IAcknowledgement acknowledgement,
            TestMessage message,
            string messageId)
        {
            Received.Add((messageId, message));

            await _acknowledgementTask(acknowledgement).ConfigureAwait(false);
        }
    }
}