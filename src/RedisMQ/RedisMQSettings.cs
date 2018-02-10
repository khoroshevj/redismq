namespace RedisMQ
{
    public class RedisMQSettings
    {
        public string TasksQueueName { get; set; }
        public string ProcessingQueuePrefix { get; set; }
        public string DeadLetterQueue { get; set; }
        public int InstancesCount { get; set; }
        
        public int LookupDelaySeconds { get; set; }
    }
}