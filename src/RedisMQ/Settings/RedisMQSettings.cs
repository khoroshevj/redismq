namespace RedisMQ
{
    /// <summary>
    /// MQ settings
    /// </summary>
    public class RedisMQSettings
    {
        public string ConnectionString { get; set; }
        
        /// <summary>
        /// Key of redis list that will hold messages
        /// </summary>
        public string TasksQueueName { get; set; }
        
        /// <summary>
        /// Prefix for queue of messages which are proccessing now.
        /// Will be used to build unique name for each instance
        /// </summary>
        public string ProcessingQueuePrefix { get; set; }
        
        /// <summary>
        /// Number of instances
        /// </summary>
        public int InstancesCount { get; set; }
        
        /// <summary>
        /// Queue which holds messages that cannot be processed
        /// </summary>
        public string DeadLetterQueue { get; set; }
        
        /// <summary>
        /// Period to wait before next looking for idle or orphan processing queues
        /// </summary>
        public int LookupDelayMilliseconds { get; set; }
    }
}