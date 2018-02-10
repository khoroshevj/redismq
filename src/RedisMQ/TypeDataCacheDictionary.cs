using System;
using System.Collections.Generic;

namespace RedisMQ
{
    public abstract class TypeDataCacheDictionary
    {
        private readonly Dictionary<Type, string> _cache = new Dictionary<Type, string>();

        public string Get(Type type)
        {
            if (_cache.TryGetValue(type, out var value))
            {
                return value;
            }
            else
            {
                var data = GetData(type);

                _cache[type] = data;

                return data;
            }
        }

        protected abstract string GetData(Type type);
    }
}