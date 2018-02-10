namespace RedisMQ
{
    public interface IRedisMessageKeyBuilder
    {
        string GetMessageKey(MessageProperties properties);
        MessageProperties GetMessageProperties(string key);
    }
    
    public class DefaultRedisMessageKeyBuilder : IRedisMessageKeyBuilder
    {
        private readonly string _delimeter;

        public DefaultRedisMessageKeyBuilder(string delimeter = ":")
        {
            _delimeter = delimeter;
        }

        public string GetMessageKey(MessageProperties properties)
        {
            return $"{properties.PayloadType}{_delimeter}{properties.MessageId}";
        }
        
        public MessageProperties GetMessageProperties(string key)
        {
            var parts = key.Split(_delimeter);

            return new MessageProperties(
                payloadType: parts[0],
                messageId: parts[1]);
        }
    }

    public struct MessageProperties
    {
        public string PayloadType { get; }
        public string MessageId { get; }

        public MessageProperties(string payloadType, string messageId)
        {
            PayloadType = payloadType;
            MessageId = messageId;
        }
    }
}