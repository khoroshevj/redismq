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
            RedisMQSettings mqSettings)
        {
            _multiplexer = ConnectionMultiplexer.Connect(connectionString);

            _logger = logger;
            _mqSettings = mqSettings;
            _lockTtl = TimeSpan.FromSeconds(_mqSettings.LookupDelaySeconds + 10);

            _lockToken = Guid.NewGuid().ToString();
            
            _consumer = new RedisMessagesConsumer(
                logger,
                _multiplexer,
                _mqSettings.TasksQueueName,
                _mqSettings.DeadLetterQueue);
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
                    var db = _multiplexer.GetDatabase();
                    for (var i = 0; i < _mqSettings.InstancesCount; i++)
                    {
                        var processingQueueName = $"{_mqSettings.ProcessingQueuePrefix}__{i}";
                        var processingQueueLock = $"queuelock:{processingQueueName}";
                        var lockAcquired = await db.LockTakeAsync(processingQueueLock, _lockToken, _lockTtl);
                        if (lockAcquired)
                        {
                            if (!_captured)
                            {
                                _consumer.Start(processingQueueName);
                                _captured = true;
                                _capturedQueue = processingQueueName;
                            }
                            else
                            {
                                await MoveMessagesBackToTasksAsync(db, processingQueueName, processingQueueLock);
                            }
                        }
                        else if (_captured && processingQueueName == _capturedQueue)
                        {
                            await db.LockExtendAsync(processingQueueLock, _lockToken, _lockTtl);
                        }
                    }

                    await Task.Delay(_mqSettings.LookupDelaySeconds * 1000);
                }
            })
            {
                IsBackground = true
            };

            thread.Start();
        }

        private async Task MoveMessagesBackToTasksAsync(IDatabase db, string processingQueueName, string processingQueueLock)
        {
            try
            {
                RedisValue popedValue;
                do
                {
                    popedValue = await db.ListRightPopLeftPushAsync(processingQueueName, _mqSettings.TasksQueueName);
                } while (popedValue.HasValue);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "");
            }
            finally
            {
                await db.LockReleaseAsync(processingQueueLock, _lockToken);
            }
        }
    }
}