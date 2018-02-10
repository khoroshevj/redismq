using System;
using System.Reflection;

namespace RedisMQ
{
    public static class RedisQueueMessagePayloadTypeNameCache
    {
        public static string Get(Type type)
            => type.GetTypeInfo().GetCustomAttribute<RedisQueueMessageAttribute>().PayloadTypeName;
    }

    public static class RedisQueueMessagePayloadTypeNameCache<T>
    {
        private static readonly Lazy<string> _name
            = new Lazy<string>(() => RedisQueueMessagePayloadTypeNameCache.Get(typeof(T)));

        public static string Name => _name.Value;

        static RedisQueueMessagePayloadTypeNameCache()
        {
        }
    }
}