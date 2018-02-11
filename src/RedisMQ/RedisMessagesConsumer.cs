using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedisMQ
{
    internal interface IRedisMessagesConsumer : IDisposable
    {
        void RegisterMessageHandler(string payloadType, IRedisMessageHandler handler);
        void Start(string processingQueue);
    }
    
    internal class RedisMessagesConsumer : IRedisMessagesConsumer
    {
        private readonly Dictionary<string, IRedisMessageHandler> _handlers;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly ILogger _logger;
        private readonly IConnectionMultiplexer _multiplexer;
        private readonly IRedisMessageKeyBuilder _keyBuilder;

        private readonly string _tasksQueue;
        private readonly string _deadLetterQueue;

        public RedisMessagesConsumer(
            ILogger logger,
            IConnectionMultiplexer multiplexer,
            string tasksQueue,
            string deadLetterQueue,
            IRedisMessageKeyBuilder keyBuilder)
        {
            _handlers = new Dictionary<string, IRedisMessageHandler>();
            
            _logger = logger;
            _multiplexer = multiplexer;
            _tasksQueue = tasksQueue;
            _deadLetterQueue = deadLetterQueue;
            _keyBuilder = keyBuilder;
        }

        public void RegisterMessageHandler(string payloadType, IRedisMessageHandler handler)
        {
            lock (_handlers)
            {
                if (_handlers.ContainsKey(payloadType))
                {
                    var message = $"Handler for messages with type '{payloadType}' has been already added";
                    _logger?.LogError(message);
                    throw new ArgumentException(message);
                }

                _handlers.Add(payloadType, handler);
            }
        }

        public void Start(string processingQueue)
        {
            var thread = new Thread(async () =>
            {
                var db = _multiplexer.GetDatabase();
                while (!_cts.IsCancellationRequested)
                {
                    var key = await db.ListRightPopLeftPushAsync(_tasksQueue, processingQueue)
                        .ConfigureAwait(false);

                    if (key.HasValue)
                    {
                        var message = await db.StringGetAsync(key.ToString());
                        await ProcessMessageAsync(processingQueue, key, message)
                            .ConfigureAwait(false);
                    }
                }
            })
            {
                IsBackground = true
            };

            thread.Start();
        }
        
        private async Task ProcessMessageAsync(string processingQueue, string key, string message)
        {
            try
            {
                var messageProperties = _keyBuilder.GetMessageProperties(key);
                var payloadType = messageProperties.PayloadType;
                if (!string.IsNullOrEmpty(payloadType) && _handlers.TryGetValue(payloadType, out var handler))
                {
                    var acknowledgement = new RedisAcknowledgement(
                        _multiplexer,
                        key,
                        processingQueue,
                        _tasksQueue,
                        _deadLetterQueue);

                    await handler.HandleMessageAsync(acknowledgement, message, messageProperties.MessageId)
                        .ConfigureAwait(false);
                }
                else
                {
                    var db = _multiplexer.GetDatabase();
                    var transaction = db.CreateTransaction();
            
#pragma warning disable 4014
                    transaction.ListRemoveAsync(processingQueue, key, 1);
                    transaction.ListLeftPushAsync(_deadLetterQueue, key);
#pragma warning restore 4014
            
                    await transaction.ExecuteAsync()
                        .ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                _logger?.LogError(e, "unhandled");
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
        }
    }
}