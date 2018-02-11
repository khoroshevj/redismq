using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedisMQ.Helpers;
using StackExchange.Redis;

namespace RedisMQ
{
    public class RedisMessagesConsumerManager : IDisposable
    {
        private readonly IRedisMessagesConsumer _consumer;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly ConnectionMultiplexer _multiplexer;
        private readonly ILogger<RedisMessagesConsumerManager> _logger;
        
        private readonly RedisMQSettings _mqSettings;
        
        private readonly TimeSpan _lockTtl;
        
        private bool _captured;
        private string _capturedQueue;
        private readonly string _lockToken;

        public RedisMessagesConsumerManager(
            ILogger<RedisMessagesConsumerManager> logger,
            string connectionString,
            RedisMQSettings mqSettings)
            : this(logger, connectionString, mqSettings, new DefaultRedisMessageKeyBuilder())
        {

        }

        public RedisMessagesConsumerManager(
            ILogger<RedisMessagesConsumerManager> logger,
            string connectionString,
            RedisMQSettings mqSettings,
            IRedisMessageKeyBuilder keyBuilder)
        {
            ThrowHelper.ThrowIfNull(logger, nameof(logger));
            ThrowHelper.ThrowIfNull(connectionString, nameof(connectionString));
            ThrowHelper.ThrowIfNull(mqSettings, nameof(mqSettings));
            ThrowHelper.ThrowIfNull(keyBuilder, nameof(keyBuilder));
            
            const int lockReserve = 500;
            
            _mqSettings = mqSettings;
            
            _multiplexer = ConnectionMultiplexer.Connect(connectionString);
            _logger = logger;
            
            _lockTtl = TimeSpan.FromMilliseconds(_mqSettings.LookupDelayMilliseconds + lockReserve);
            _lockToken = Guid.NewGuid().ToString();
            
            _consumer = new RedisMessagesConsumer(
                logger,
                _multiplexer,
                _mqSettings.TasksQueueName,
                _mqSettings.DeadLetterQueue,
                keyBuilder);
        }

        public void Start()
        {
            StartIdleQueueLookup();
        }
        
        /// <summary>
        /// Register handler for type
        /// Important! Only one handler for type permitted
        /// </summary>
        /// <param name="type">type with RedisQueueMessageAttribute</param>
        /// <param name="handler">handler</param>
        /// <exception cref="ArgumentException"> Handler for that type of messages  has been already added </exception>
        public void RegisterMessageHandler(Type type, IRedisMessageHandler handler)
        {
            var name = RedisQueueMessagePayloadTypeNameCache.Get(type);

            _consumer.RegisterMessageHandler(name, handler);
        }

        /// <summary>
        /// Register handler for type
        /// Important! Only one handler for type permitted
        /// </summary>
        /// <param name="type">type with RedisQueueMessageAttribute</param>
        /// <param name="handler">handler</param>
        /// <exception cref="ArgumentException"> Handler for that type of messages  has been already added </exception>
        public void RegisterMessageHandler<TMessage>(IRedisMessageHandler<TMessage> handler)
        {
            RegisterMessageHandler(typeof(TMessage), handler);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _consumer.Dispose();
            _multiplexer.Dispose();
        }

        private void StartIdleQueueLookup()
        {
            var thread = new Thread(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    for (var i = 0; i < _mqSettings.InstancesCount; i++)
                    {
                        var db = _multiplexer.GetDatabase();
                        var processingQueueName = $"{_mqSettings.ProcessingQueuePrefix}__{i}";
                        var processingQueueLock = $"queuelock:{processingQueueName}";
                        
                        if (_captured && processingQueueName == _capturedQueue)
                        {
                            _logger.LogTrace($"{_lockToken} | Continue processing {i} queue");
                            
                            await db.LockExtendAsync(processingQueueLock, _lockToken, _lockTtl)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            _logger.LogTrace($"{_lockToken} | Trying to capture lock for {i} queue");
                            
                            var lockAcquired = await db.LockTakeAsync(processingQueueLock, _lockToken, _lockTtl)
                                .ConfigureAwait(false);
                            
                            if (lockAcquired)
                            {
                                _logger.LogTrace($"{_lockToken} | Captured lock for {i} queue");
                                
                                _logger.LogTrace($"{_lockToken} | Moving messages from {i} to tasks queue");
                                await MoveMessagesBackToTasksAsync(db, processingQueueName);
                                if (_captured)
                                {
                                    await db.LockReleaseAsync(processingQueueLock, _lockToken)
                                        .ConfigureAwait(false);
                                }
                                else
                                {
                                    _logger.LogTrace($"{_lockToken} | Starting to process {processingQueueName} queue");

                                    _captured = true;
                                    _capturedQueue = processingQueueName;
                                    _consumer.Start(processingQueueName);

                                    await db.LockExtendAsync(processingQueueLock, _lockToken, _lockTtl)
                                        .ConfigureAwait(false);
                                }
                            }
                        }
                    }

                    await Task.Delay(_mqSettings.LookupDelayMilliseconds)
                        .ConfigureAwait(false);
                }
                
                if (_captured)
                {
                    await _multiplexer.GetDatabase()
                    .LockReleaseAsync(_capturedQueue, _lockToken)
                    .ConfigureAwait(false);
                }

                _logger.LogTrace($"{_lockToken} | Cancelled for token ");
            })
            {
                IsBackground = true
            };

            thread.Start();
        }

        private async Task MoveMessagesBackToTasksAsync(IDatabase db, string processingQueueName)
        {
            RedisValue popedValue;
            do
            {
                popedValue = await db.ListRightPopLeftPushAsync(processingQueueName, _mqSettings.TasksQueueName);
            } while (popedValue.HasValue);
        }
    }
}