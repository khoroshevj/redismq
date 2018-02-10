using Newtonsoft.Json;

namespace RedisMQ
{
    public interface IRedisMQMessageSerializer
    {
        string Serialize<T>(T value);
        T Deserialize<T>(string value);
    }
    
    public sealed class SimpleJsonSerializer : IRedisMQMessageSerializer
    {
        public string Serialize<T>(T value)
        {
            return JsonConvert.SerializeObject(value);
        }

        public T Deserialize<T>(string value)
        {
            return JsonConvert.DeserializeObject<T>(value);
        }
    }
}