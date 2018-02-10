using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
            RedisMQSettings mqSettings,
            IRedisMessageKeyBuilder keyBuilder)
        {
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
        /// Добавить обработчик сообщения.
        /// На один тип сообщения, должен быть один обработчик
        /// </summary>
        /// <param name="type">Тип события</param>
        /// <param name="handler">класс обработчик</param>
        /// <exception cref="ArgumentException"> обработчик для этого типа сообщений уже добавлен </exception>
        public void RegisterMessageHandler(Type type, IRedisMessageHandler handler)
        {
            var name = RedisQueueMessagePayloadTypeNameCache.Get(type);

            _consumer.RegisterMessageHandler(name, handler);
        }

        /// <summary>
        /// Добавить обработчик сообщения.
        /// На один тип сообщения, должен быть один обработчик
        /// </summary>
        /// <typeparam name="TMessage"> сообщение, долно быть помечено аттрибутом RedisQueueMessageAttribute </typeparam>
        /// <param name="handler">класс обработчик</param>
        /// <exception cref="ArgumentException"> обработчик для этого типа сообщений уже добавлен </exception>
        public void RegisterMessageHandler<TMessage>(IRedisMessageHandler<TMessage> handler)
        {
            RegisterMessageHandler(typeof(TMessage), handler);
        }

        public void Dispose()
        {
            _consumer?.Dispose();
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
                            await db.LockExtendAsync(processingQueueLock, _lockToken, _lockTtl)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            var lockAcquired = await db.LockTakeAsync(processingQueueLock, _lockToken, _lockTtl)
                                .ConfigureAwait(false);

                            if (lockAcquired)
                            {
                                await MoveMessagesBackToTasksAsync(db, processingQueueName);
                                if (!_captured)
                                {
                                    _captured = true;
                                    _capturedQueue = processingQueueName;
                                    _consumer.Start(processingQueueName);
                                    
                                    await db.LockExtendAsync(processingQueueLock, _lockToken, _lockTtl)
                                        .ConfigureAwait(false);
                                }
                                else
                                {
                                    await db.LockReleaseAsync(processingQueueLock, _lockToken)
                                        .ConfigureAwait(false);
                                }
                            }
                        }
                    }

                    /*await Task.Delay(_mqSettings.LookupDelayMilliseconds)
                        .ConfigureAwait(false);*/
                }
                
                _logger.LogTrace("");
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