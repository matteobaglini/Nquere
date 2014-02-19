using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nquere.Tests
{
    public class ContinuoslyConsumerTests : LocalHostRedisConnection
    {
        [Fact]
        public void ProcessItems()
        {
            const string queueName = "test:queue";
            var items = new[] { "test-value1", "test-value2" };
            var values = new List<String>();
            var done = new CountdownEvent(items.Length);
            var queue = new RedisQueue(Connection);

            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.Push(DefaultDb, queueName, items[1]).Wait();

            queue.StartString(DefaultDb, queueName, OneSecond, value =>
            {
                values.Add(value);
                done.Signal();
            });

            done.Wait(TimeSpan.FromSeconds(5));
            queue.Stop(DefaultDb, queueName);

            Assert.Equal(items, values);
        }

        [Fact]
        public void ProcessEmpty()
        {
            const string queueName = "test:queue";
            var invokeCount = 0;
            var queue = new RedisQueue(Connection);

            queue.StartString(DefaultDb, queueName, OneSecond, value => invokeCount++);

            Task.Delay(TimeSpan.FromSeconds(OneSecond * 2)).Wait();
            queue.Stop(DefaultDb, queueName);

            Assert.Equal(0, invokeCount);
        }

        [Fact]
        public void PushItemsAfterStartedEmpty()
        {
            const string queueName = "test:queue";
            var items = new[] { "test-value1", "test-value2" };
            var invokeCount = 0;
            var done = new CountdownEvent(items.Length);
            var queue = new RedisQueue(Connection);

            queue.StartString(DefaultDb, queueName, OneSecond, value =>
            {
                invokeCount++;
                done.Signal();
            });

            Task.Delay(TimeSpan.FromSeconds(OneSecond * 2)).Wait();
            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.Push(DefaultDb, queueName, items[1]).Wait();

            done.Wait(TimeSpan.FromSeconds(5));
            queue.Stop(DefaultDb, queueName);

            Assert.Equal(2, invokeCount);
        }

        [Fact]
        public void ProcessBinaryItems()
        {
            const string queueName = "test:queue";
            Func<String, Byte[]> getBytes = Encoding.ASCII.GetBytes;
            var items = new[] { getBytes("test-value1"), getBytes("test-value2") };
            var values = new List<Byte[]>();
            var done = new CountdownEvent(items.Length);
            var queue = new RedisQueue(Connection);

            queue.Push(DefaultDb, queueName, items[0]).Wait();
            queue.Push(DefaultDb, queueName, items[1]).Wait();

            queue.Start(DefaultDb, queueName, OneSecond, value =>
            {
                values.Add(value);
                done.Signal();
            });

            done.Wait(TimeSpan.FromSeconds(5));
            queue.Stop(DefaultDb, queueName);

            Assert.Equal(items, values);
        }

        [Fact]
        public void ExceptionBubbling()
        {
            const string queueName = "test:queue";
            var queue = new RedisQueue(Connection);

            queue.Push(DefaultDb, queueName, "not useful").Wait();

            queue.StartString(DefaultDb, queueName, OneSecond, value => { throw new DivideByZeroException(); });

            Task.Delay(TimeSpan.FromSeconds(OneSecond)).Wait();
            var ex = Record.Exception(() => queue.Stop(DefaultDb, queueName).Wait());

            Assert.IsType<AggregateException>(ex);
            Assert.IsType<DivideByZeroException>(ex.GetBaseException());
        }

        [Fact]
        public void DoubleStartOnSameSource()
        {
            const string queueName = "test:queue";
            var queue = new RedisQueue(Connection);

            queue.Start(DefaultDb, queueName, OneSecond, value => { });
            var ex = Record.Exception(() => queue.Start(DefaultDb, queueName, OneSecond, value => { }));

            Assert.IsType<InvalidOperationException>(ex);
            Assert.Contains(DefaultDb.ToString(), ex.Message, StringComparison.InvariantCultureIgnoreCase);
            Assert.Contains(queueName, ex.Message, StringComparison.InvariantCultureIgnoreCase);
        }

        [Fact]
        public void StopOnUnstartedSource()
        {
            const string queueName = "test:queue";
            var queue = new RedisQueue(Connection);

            var ex = Record.Exception(() => queue.Stop(DefaultDb, queueName));

            Assert.IsType<InvalidOperationException>(ex);
            Assert.Contains(DefaultDb.ToString(), ex.Message, StringComparison.InvariantCultureIgnoreCase);
            Assert.Contains(queueName, ex.Message, StringComparison.InvariantCultureIgnoreCase);
        }
    }
}