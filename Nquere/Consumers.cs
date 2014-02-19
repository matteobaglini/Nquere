using System;
using System.Collections.Generic;

namespace Nquere
{
    internal class Consumers
    {
        private readonly Dictionary<Tuple<Int32, String>, IConsumer> consumers;

        public Consumers()
        {
            consumers = new Dictionary<Tuple<Int32, String>, IConsumer>();
        }

        public void Add(Int32 db, String queueName, IConsumer consumer)
        {
            var key = ConsumerKey(db, queueName);
            lock (consumers)
            {
                if (consumers.ContainsKey(key)) throw new InvalidOperationException(String.Format("There is already a consumer on the requested queue (db={0} queueName={1}).", db, queueName));
                consumers.Add(key, consumer);
            }
        }

        public IConsumer Get(Int32 db, String queueName)
        {
            var key = ConsumerKey(db, queueName);
            IConsumer consumer;
            lock (consumers)
            {
                if (!consumers.ContainsKey(key)) throw new InvalidOperationException(String.Format("There isn't a consumer on the requested queue (db={0} queueName={1}).", db, queueName));
                consumer = consumers[key];
            }
            return consumer;
        }

        //public IConsumer Put(Int32 db, String queueName, Func<IConsumer> consumerFactory)
        //{
        //    var key = ConsumerKey(db, queueName);
        //    IConsumer consumer;
        //    lock (consumers)
        //    {
        //        if (consumers.ContainsKey(key)) throw new InvalidOperationException(String.Format("There is already a consumer on the requested queue (db={0} queueName={1}).", db, queueName));
        //        consumer = consumerFactory();
        //        consumers.Add(key, consumer);
        //    }
        //    return consumer;
        //}

        //public IConsumer Get(Int32 db, String queueName, Action<IConsumer> consumerAction)
        //{
        //    var key = ConsumerKey(db, queueName);
        //    IConsumer consumer;
        //    lock (consumers)
        //    {
        //        if (!consumers.ContainsKey(key)) throw new InvalidOperationException(String.Format("There isn't a consumer on the requested queue (db={0} queueName={1}).", db, queueName));
        //        consumer = consumers[key];
        //        consumerAction(consumer);
        //    }
        //    return consumer;
        //}

        private static Tuple<Int32, String> ConsumerKey(Int32 db, String queueName)
        {
            return Tuple.Create(db, queueName);
        }
    }
}