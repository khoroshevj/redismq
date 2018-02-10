using Newtonsoft.Json;
using StackExchange.Redis;

namespace RedisMQ
{
    public interface IRedisMessageSender
    {
        void Send<TMessage>(ITransaction transaction, string messageId, TMessage message);
    }
    
    public class RedisMessageSender : IRedisMessageSender
    {
        private readonly string _tasksQueue;

        public RedisMessageSender(string tasksQueue)
        {
            _tasksQueue = tasksQueue;
        }

        public void Send<TMessage>(ITransaction transaction, string messageId, TMessage message)
        {
            var payloadType = RedisQueueMessagePayloadTypeNameCache<TMessage>.Name;
            var messageKey = $"{payloadType}:{messageId}";
            
            transaction.StringSetAsync(messageKey, JsonConvert.SerializeObject(message));
            transaction.ListLeftPushAsync(_tasksQueue, messageKey);
        }
    }
}