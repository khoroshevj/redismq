using StackExchange.Redis;

namespace RedisMQ
{
    public interface IRedisMessageSender
    {
        void Send<TMessage>(ITransaction transaction, string messageId, TMessage message);
    }
    
    public class RedisMessageSender : IRedisMessageSender
    {
        private readonly IRedisMQMessageSerializer _serializer;
        private readonly string _tasksQueue;

        public RedisMessageSender(string tasksQueue)
            : this(tasksQueue, null)
        {
        }

        public RedisMessageSender(string tasksQueue, IRedisMQMessageSerializer serializer)
        {
            _serializer = serializer ?? new SimpleJsonSerializer();
            _tasksQueue = tasksQueue;
        }

        public void Send<TMessage>(ITransaction transaction, string messageId, TMessage message)
        {
            var payloadType = RedisQueueMessagePayloadTypeNameCache<TMessage>.Name;
            var messageKey = $"{payloadType}:{messageId}";
            
            transaction.StringSetAsync(messageKey, _serializer.Serialize(message));
            transaction.ListLeftPushAsync(_tasksQueue, messageKey);
        }
    }
}