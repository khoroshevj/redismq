using System;

namespace RedisMQ
{
    [AttributeUsage(AttributeTargets.Class)]
    public class RedisQueueMessageAttribute : Attribute
    {
        public string PayloadTypeName { get; }
        
        public RedisQueueMessageAttribute(string payloadTypeName)
        {
            PayloadTypeName = payloadTypeName;
        }
    }
}