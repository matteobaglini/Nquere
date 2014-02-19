using System;
using System.Text;
using Xunit;

namespace Nquere.Tests
{
    public class OneByOneConsumerTests : LocalHostRedisConnection
    {
        [Fact]
        public void ProcessItems()
        {
            const string queueName = "test:queue";
            var items = new[] { "test-value1", "test-value2" };
            var queue = new RedisQueue(Connection);

            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.Push(DefaultDb, queueName, items[1]).Wait();

            var values = new[]{ queue.PopString(DefaultDb, queueName, OneSecond).Result,
                queue.PopString(DefaultDb, queueName, OneSecond).Result};

            Assert.Equal(items, values);
        }

        [Fact]
        public void ProcessEmpty()
        {
            const string queueName = "test:queue";
            var queue = new RedisQueue(Connection);

            var value = queue.PopString(DefaultDb, queueName, OneSecond).Result;

            Assert.Null(value);
        }

        [Fact]
        public void ProcessBinaryItems()
        {
            const string queueName = "test:queue";
            Func<String, Byte[]> getBytes = Encoding.ASCII.GetBytes;
            var items = new[] { getBytes("test-value1"), getBytes("test-value2") };
            var queue = new RedisQueue(Connection);

            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.Push(DefaultDb, queueName, items[1]).Wait();

            var values = new[] { queue.Pop(DefaultDb, queueName, OneSecond).Result,
                queue.Pop(DefaultDb, queueName, OneSecond).Result};

            Assert.Equal(items, values);
        }
    }
}