using System;
using System.Threading.Tasks;
using BookSleeve;

namespace Nquere
{
    public class RedisQueue
    {
        private readonly RedisConnection connection;
        private readonly Consumers consumers;

        public RedisQueue(RedisConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            this.connection = connection;
            consumers = new Consumers();
        }

        public Task Push(Int32 db, String queueName, Byte[] value)
        {
            return connection.Lists.AddFirst(db, queueName, value);
        }

        public Task Push(Int32 db, String queueName, String value)
        {
            return connection.Lists.AddFirst(db, queueName, value);
        }

        public Task<Byte[]> Pop(Int32 db, String queueName, Int32 timeoutInSecs)
        {
            var tcs = new TaskCompletionSource<Byte[]>();
            connection
                .Lists
                .BlockingRemoveLast(db, new[] { queueName }, timeoutInSecs)
                .ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        tcs.TrySetException(t.Exception);
                    else if (t.IsCanceled)
                        tcs.TrySetCanceled();
                    else
                        tcs.TrySetResult(t.Result == null ? null : t.Result.Item2);
                }, TaskContinuationOptions.ExecuteSynchronously);
            return tcs.Task;
        }

        public Task<String> PopString(Int32 db, String queueName, Int32 timeoutInSecs)
        {
            var tcs = new TaskCompletionSource<String>();
            connection
                .Lists
                .BlockingRemoveLastString(db, new[] { queueName }, timeoutInSecs)
                .ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        tcs.TrySetException(t.Exception);
                    else if (t.IsCanceled)
                        tcs.TrySetCanceled();
                    else
                        tcs.TrySetResult(t.Result == null ? null : t.Result.Item2);
                }, TaskContinuationOptions.ExecuteSynchronously);
            return tcs.Task;
        }

        public Task Start(Int32 db, String queueName, Int32 timeoutInSecs, Action<Byte[]> action)
        {
            if (action == null) throw new ArgumentNullException("action");
            var consumer = AsyncConsumer.Bytes(connection, db, queueName, timeoutInSecs, action);
            consumers.Add(db, queueName, consumer);
            consumer.Start();
            return consumer.Task;
        }

        public Task StartString(Int32 db, String queueName, Int32 timeoutInSecs, Action<String> action)
        {
            if (action == null) throw new ArgumentNullException("action");
            var consumer = AsyncConsumer.String(connection, db, queueName, timeoutInSecs, action);
            consumers.Add(db, queueName, consumer);
            consumer.Start();
            return consumer.Task;
        }

        public Task Stop(Int32 db, String queueName)
        {
            var consumer = consumers.Get(db, queueName);
            consumer.Stop();
            return consumer.Task;
        }
    }
}