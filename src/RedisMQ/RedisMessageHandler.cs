using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedisMQ.Helpers;

namespace RedisMQ
{
    public interface IRedisMessageHandler
    {
        Task HandleMessageAsync(IAcknowledgement acknowledgement, string message, string messageId);
    }
    
    public interface IRedisMessageHandler<in T> : IRedisMessageHandler
    {
        Task HandleMessageAsync(IAcknowledgement acknowledgement, T message, string messageId);
    }

    public abstract class RedisMessageHandlerBase<T> : IRedisMessageHandler<T>
    {
        private readonly IRedisMQMessageSerializer _messageSerializer;
        
        protected ILogger Logger { get; }

        protected RedisMessageHandlerBase(ILogger logger)
            : this(logger, new SimpleJsonSerializer())
        {
        }

        protected RedisMessageHandlerBase(
            ILogger logger,
            IRedisMQMessageSerializer messageSerializer)
        {
            ThrowHelper.ThrowIfNull(logger, nameof(logger));
            ThrowHelper.ThrowIfNull(messageSerializer, nameof(messageSerializer));
            
            Logger = logger;
            _messageSerializer = messageSerializer;
        }
        
        async Task IRedisMessageHandler.HandleMessageAsync(IAcknowledgement acknowledgement, string message, string messageId)
        {
            T deserialized;
            try
            {
                deserialized = Deserialize(message);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception, "error deserializing message");
                await acknowledgement.NackAsync();
                return;
            }

            try
            {
                await HandleMessageAsync(acknowledgement, deserialized, messageId);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception, "error handling message", new Dictionary<string, object> { { "message", message } });
                await acknowledgement.RequeueAsync();
            }
        }

        public abstract Task HandleMessageAsync(IAcknowledgement acknowledgement, T message, string messageId);
        
        protected virtual T Deserialize(string message)
        {
            return _messageSerializer.Deserialize<T>(message);
        }
    }
}    