using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WorkQueue.Consumer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var factory = new ConnectionFactory {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // We declare queue as well because we might start
                    // the consumer before the publisher, we want to make sure
                    // the queue exists before we try to consume messages from it.

                    channel.QueueDeclare(queue: "durable_work_queue",
                        durable: true,      // durable queue
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    // RabbitMQ just dispatches a message when the message 
                    // enters the queue. It doesn't look at the number of 
                    // unacknowledged messages for a consumer. It just blindly 
                    // dispatches every n-th message to the n-th consumer.

                    // prefetchCount = 1 tells RabbitMQ don't dispatch a new 
                    // message to a worker until it has processed and acknowledged 
                    // the previous one

                    // NOTE: If all the workers are busy, your queue can fill up.
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    Console.WriteLine(" [*] Waiting for messages.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);

                        var dots = message.Split('.').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine(" [x] Done");

                        // In order to make sure a message is never lost, 
                        // RabbitMQ supports message acknowledgments. 
                        // There aren't any message timeouts; RabbitMQ will redeliver 
                        // the message when the consumer dies.
                        channel.BasicAck(
                            deliveryTag: ea.DeliveryTag, 
                            multiple: false);
                    };

                    channel.BasicConsume(
                        queue: "durable_work_queue", 
                        autoAck: false,         // false for manual ackn
                        consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}
