using System;
using System.Diagnostics;
using System.Threading.Tasks;
using BookSleeve;

namespace Nquere
{
    internal class AsyncConsumer
    {
        public static IConsumer Bytes(RedisConnection connection, int db, string queueName, int timeout, Action<Byte[]> action)
        {
            return new GenericAsyncConsumer<Byte[]>(db, queueName, timeout, action,
                connection.Lists.BlockingRemoveLastAndAddFirst,
                connection.Lists.RemoveLast,
                connection.Lists.Range);
        }

        public static IConsumer String(RedisConnection connection, int db, string queueName, int timeout, Action<String> action)
        {
            return new GenericAsyncConsumer<String>(db, queueName, timeout, action,
                connection.Lists.BlockingRemoveLastAndAddFirstString,
                connection.Lists.RemoveLastString,
                connection.Lists.RangeString);
        }

        private class GenericAsyncConsumer<TResult> : IConsumer where TResult : class
        {
            private readonly int db;
            private readonly string queueName;
            private readonly int timeout;
            private readonly Action<TResult> action;
            private readonly string backupName;
            private readonly TaskCompletionSource<object> tcs;
            private readonly Func<int, string, string, int, bool, Task<TResult>> popValue;
            private readonly Func<int, string, bool, Task<TResult>> popBackup;
            private readonly Func<int, string, int, int, bool, Task<TResult[]>> rangeBackup;
            private readonly bool queueJump;

            private volatile Boolean continueConsume;

            public Task Task
            {
                get { return tcs.Task; }
            }

            public GenericAsyncConsumer(int db, string queueName, int timeout, Action<TResult> action, Func<int, string, string, int, bool, Task<TResult>> popValue, Func<int, string, bool, Task<TResult>> popBackup, Func<int, string, int, int, bool, Task<TResult[]>> rangeBackup)
            {
                this.db = db;
                this.queueName = queueName;
                this.timeout = timeout;
                this.action = action;
                this.popValue = popValue;
                this.popBackup = popBackup;
                this.rangeBackup = rangeBackup;
                this.queueJump = false;
                backupName = System.String.Format("{0}:{1}:{2}", Environment.MachineName, Process.GetCurrentProcess().Id, queueName);
                tcs = new TaskCompletionSource<object>();
            }

            public void Start()
            {
                continueConsume = true;
                Task.Factory.StartNew(ConsumeNext);
            }

            public void Stop()
            {
                continueConsume = false;
            }

            public Task<TResult[]> BackupItems()
            {
                return rangeBackup(db, backupName, 0, -1, queueJump);
            }

            private void ConsumeNext()
            {
                if (continueConsume)
                {
                    try
                    {
                        popValue(db, queueName, backupName, timeout, queueJump)
                            .ContinueWith(PopCompleted);
                    }
                    catch (Exception ex)
                    {
                        Failure(ex);
                    }
                }
                else
                    Done();
            }

            private void PopCompleted(Task<TResult> antecedent)
            {
                if (antecedent.IsFaulted)
                    Failure(antecedent.Exception);
                else if (antecedent.IsCanceled)
                    Cancel();
                else if (antecedent.IsCompleted)
                    if (antecedent.Result != null)
                    {
                        try
                        {
                            action(antecedent.Result);
                            popBackup(db, backupName, queueJump)
                                .ContinueWith(PopBackupCompleted);
                        }
                        catch (Exception ex)
                        {
                            Failure(ex);
                        }
                    }
                    else
                        ConsumeNext();
            }

            private void PopBackupCompleted(Task<TResult> antecedent)
            {
                if (antecedent.IsFaulted)
                    Failure(antecedent.Exception);
                else if (antecedent.IsCanceled)
                    Cancel();
                else if (antecedent.IsCompleted)
                    ConsumeNext();
            }

            private void Done()
            {
                tcs.TrySetResult(null);
            }

            private void Cancel()
            {
                continueConsume = false;
                tcs.TrySetCanceled();
            }

            private void Failure(Exception ex)
            {
                continueConsume = false;
                tcs.TrySetException(ex);
            }
        }
    }
}