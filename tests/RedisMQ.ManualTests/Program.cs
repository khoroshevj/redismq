using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace RedisMQ.ManualTests
{
    public class Program
    {
        private static ConnectionMultiplexer _multiplexer;
        private static string _uniqPrefix;
        private static string _tasksQueue;
        private static string _processingQueuePrefix;

        public static void Main(string[] args)
        {
            var connectionString = "127.0.0.1:6379,abortConnect=false";
            
            _multiplexer = ConnectionMultiplexer.Connect(connectionString);
            _uniqPrefix = Guid.NewGuid().ToString("N");

            _tasksQueue = _uniqPrefix + "_TaskQueue";
            _processingQueuePrefix = _uniqPrefix + "_ProcessingQueue_";

            var consumer1 = new RedisMessagesConsumerManager(
                NullLogger<RedisMessagesConsumerManager>.Instance, 
                connectionString,
                new RedisMQSettings()
                {
                    DeadLetterQueue = "ddd",
                    InstancesCount = 2,
                    LookupDelayMilliseconds = 2 * 1000,
                    ProcessingQueuePrefix = _processingQueuePrefix,
                    TasksQueueName = _tasksQueue
                });
            
            var consumer2 = new RedisMessagesConsumerManager(
                NullLogger<RedisMessagesConsumerManager>.Instance, 
                connectionString,
                new RedisMQSettings()
                {
                    DeadLetterQueue = "ddd",
                    InstancesCount = 2,
                    LookupDelayMilliseconds = 2 * 1000,
                    ProcessingQueuePrefix = _processingQueuePrefix,
                    TasksQueueName = _tasksQueue
                });

            Console.WriteLine("registering");
            consumer2.RegisterMessageHandler(new ConsoleOuputHanlder("2", NullLogger.Instance));
            consumer1.RegisterMessageHandler(new ConsoleOuputHanlder("1", NullLogger.Instance));
            
            Console.WriteLine("starting");
            consumer1.Start();
            consumer2.Start();

            Console.WriteLine("started");
            var redisMessageSender = new RedisMessageSender(_tasksQueue);
            RunPressure(redisMessageSender);

            Console.ReadLine();
        }

        private static void RunPressure(RedisMessageSender redisMessageSender)
        {
            var thread = new Thread(async () =>
            {
                var db = _multiplexer.GetDatabase();
                for (var i = 0; i < 200; i++)
                {
                    var key = _uniqPrefix + i;
                    var value = i;

                    var redisMessage = new StringRedisMessage(value.ToString());
                    
                    var transaction = db.CreateTransaction();
                    
#pragma warning disable 4014
                    transaction.StringSetAsync(key, value);
#pragma warning restore 4014
                    redisMessageSender.Send(transaction, key, redisMessage);

                    await transaction.ExecuteAsync();

                    Console.WriteLine($"sent {i}");
                }
            })
            {
                IsBackground = true
            };

            thread.Start();
        }
        
        private class ConsoleOuputHanlder : RedisMessageHandlerBase<StringRedisMessage>
        {
            private readonly string _consumer;

            public ConsoleOuputHanlder(string consumer, ILogger logger) : base(logger)
            {
                _consumer = consumer;
            }

            public override async Task HandleMessageAsync(
                IAcknowledgement acknowledgement,
                StringRedisMessage message,
                string messageId)
            {
                Console.WriteLine($"{_consumer} {message.Value} {messageId}");
                await acknowledgement.AckAsync();
            }
        }

        [RedisQueueMessage("string")]
        private class StringRedisMessage
        {
            public string Value { get; }
            
            public StringRedisMessage(string value)
            {
                Value = value;
            }
        }
    }
}