using System;
using BookSleeve;

namespace Nquere.Tests
{
    public class LocalHostRedisConnection : IDisposable
    {
        protected const String LocalHost = "127.0.0.1";
        protected const Int32 DefaultDb = 0;
        protected const Int32 OneSecond = 1;
        
        private readonly RedisConnection conn;

        protected RedisConnection Connection
        {
            get { return conn; }
        }

        public LocalHostRedisConnection()
        {
            conn = new RedisConnection(LocalHost, allowAdmin: true);
            conn.Open().Wait();
            conn.Server.FlushDb(DefaultDb).Wait();
        }

        public void Dispose()
        {
            conn.Close(true);
            conn.Dispose();
        }
    }
}