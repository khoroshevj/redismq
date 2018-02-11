using System;

namespace RedisMQ.Helpers
{
    public static class ThrowHelper
    {
        public static void ThrowIfNull(object source, string argumentName)
        {
            if (source == null)
            {
                throw new ArgumentNullException(argumentName);
            }
        }
    }
}