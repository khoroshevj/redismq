using System.Threading.Tasks;
using StackExchange.Redis;

namespace RedisMQ
{
    public interface IAcknowledgement
    {
        Task AckAsync();
        Task NackAsync();
        Task RequeueAsync(int delayMilliseconds = 0);
    }

    public class RedisAcknowledgement : IAcknowledgement
    {
        private readonly IConnectionMultiplexer _multiplexer;
        private readonly string _tasksQueue;
        private readonly string _deadLetterQueue;
        private readonly string _processingQueue;
        private readonly string _messageKey;

        internal RedisAcknowledgement(
            IConnectionMultiplexer multiplexer,            
            string messageKey,
            string processingQueue,
            string tasksQueue,
            string deadLetterQueue)
        {
            _multiplexer = multiplexer;
            _processingQueue = processingQueue;
            _messageKey = messageKey;
            _tasksQueue = tasksQueue;
            _deadLetterQueue = deadLetterQueue;
        }

        public async Task AckAsync()
        {
            var db = _multiplexer.GetDatabase();
            await db.ListRemoveAsync(_processingQueue, _messageKey, 1);
            await db.KeyDeleteAsync(_messageKey);
        }

        public async Task NackAsync()
        {
            var db = _multiplexer.GetDatabase();
            var transaction = db.CreateTransaction();
            
#pragma warning disable 4014
            transaction.ListRemoveAsync(_processingQueue, _messageKey, 1);
            transaction.ListLeftPushAsync(_deadLetterQueue, _messageKey);
#pragma warning restore 4014
            
            await transaction.ExecuteAsync()
                .ConfigureAwait(false);
        }

        public async Task RequeueAsync(int delayMilliseconds = 0)
        {
            if (delayMilliseconds > 0)
            {
                Task.Delay(delayMilliseconds)
                    .ContinueWith(async (_, __) => { await RequeueAsync(); }, null)
                    .ConfigureAwait(false);
            }
            else
            {   
                RequeueAsync();
            }
            
            async Task RequeueAsync()
            {
                var db = _multiplexer.GetDatabase();
                var transaction = db.CreateTransaction();

#pragma warning disable 4014
                transaction.ListRemoveAsync(_processingQueue, _messageKey, 1);
                transaction.ListLeftPushAsync(_tasksQueue, _messageKey);
#pragma warning restore 4014

                await transaction.ExecuteAsync(CommandFlags.FireAndForget);
            }
        }
    }
}