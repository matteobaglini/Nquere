using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nquere.Tests
{
    public class ReliabilityTests : LocalHostRedisConnection
    {
        [Fact]
        public void FailureItemProcessing()
        {
            const string queueName = "test:queue";
            var items = new[] { "test-value1" };
            var queue = new RedisQueue(Connection);
            var backupQueueName = String.Empty;

            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.StartString(DefaultDb, queueName, OneSecond, value =>
            {
                backupQueueName = Connection.Keys.Find(DefaultDb, "*:" + queueName).Result.Single();
                throw new DivideByZeroException();
            });
            Task.Delay(TimeSpan.FromSeconds(OneSecond)).Wait();
            queue.Stop(DefaultDb, queueName);
            var values = Connection.Lists.RangeString(DefaultDb, backupQueueName, 0, -1).Result;

            Assert.Equal(items, values);
        }

        [Fact]
        public void SuccesfulItemProcessing()
        {
            const string queueName = "test:queue";
            var items = new[] { "test-value1" };
            var done = new CountdownEvent(items.Length);
            var queue = new RedisQueue(Connection);
            var backupQueueName = String.Empty;

            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.StartString(DefaultDb, queueName, OneSecond, value =>
            {
                backupQueueName = Connection.Keys.Find(DefaultDb, "*:" + queueName).Result.Single();
                done.Signal();
            });
            done.Wait(TimeSpan.FromSeconds(5));
            queue.Stop(DefaultDb, queueName);
            var values = Connection.Lists.RangeString(DefaultDb, backupQueueName, 0, -1).Result;

            Assert.Empty(values);
        }
    }
}