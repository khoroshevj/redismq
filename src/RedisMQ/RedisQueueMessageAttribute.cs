using System;

namespace RedisMQ
{
    [AttributeUsage(AttributeTargets.Class)]
    public class RedisQueueMessageAttribute : Attribute
    {
        public RedisQueueMessageAttribute(string payloadTypeName)
        {
            PayloadTypeName = payloadTypeName;
        }

        public string PayloadTypeName { get; }
    }
}