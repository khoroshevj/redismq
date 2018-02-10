using System;
using System.Threading.Tasks;
using Xunit.Sdk;

namespace RedisMQ.Tests
{
    public class WaitingAssert
    {
        public static async Task CheckBecomeTrueAsync(
            Func<Task> assertion,
            int waitIntervalMs,
            uint waitRetries)
        {
            var condition = false;
            while (!condition)
            {
                try
                {
                    await assertion();
                    condition = true;
                }
                catch (XunitException)
                {
                    if (waitRetries-- == 0)
                    {
                        throw;
                    }

                    await Task.Delay(waitIntervalMs);
                }
            }
        }
        
        public static async Task CheckBecomeTrueAsync(
            Action assertion,
            int waitIntervalMs,
            uint waitRetries)
        {
            var condition = false;
            while (!condition)
            {
                try
                {
                    assertion();
                    condition = true;
                }
                catch (XunitException)
                {
                    if (waitRetries-- == 0)
                    {
                        throw;
                    }

                    await Task.Delay(waitIntervalMs);
                }
            }
        }

        public static async Task ChecStayTrueAsync(
            Func<Task> assertion,
            int waitIntervalMs,
            uint waitRetries)
        {
            while (waitRetries-- > 0)
            {
                await assertion();
                await Task.Delay(waitIntervalMs);
            }
        }

        public static async Task ChecStayTrueAsync(
            Action assertion,
            int waitIntervalMs,
            uint waitRetries)
        {
            while (waitRetries-- > 0)
            {
                assertion();
                await Task.Delay(waitIntervalMs);
            }
        }
    }
}