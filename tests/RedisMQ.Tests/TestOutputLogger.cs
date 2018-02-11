using System;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace RedisMQ.Tests
{
    public class TestOutputLogger<T> : ILogger<T>, ILogger
    {
        private readonly ITestOutputHelper _testOutput;

        public TestOutputLogger(ITestOutputHelper testOutput)
        {
            _testOutput = testOutput;
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception exception,
            Func<TState, Exception, string> formatter)
        {
            try
            {
                _testOutput.WriteLine($"{DateTime.Now:mm:ss.fff}\t{formatter(state, exception)}");
            }
            catch { }
            
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }
    }
}