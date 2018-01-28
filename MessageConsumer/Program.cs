using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace SimpleQueue.Consumer
{
    // Unlike the publisher which publishes a single message, 
    // we'll keep the consumer running continuously to listen 
    // for messages and print them out.
    internal class Program
    {
        private static void Main()
        {
            // Setting up is the same as the publisher: 
            // we open a connection and a channel,
            // and declare the queue from which we're going to consume.

            var factory = new ConnectionFactory { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // We declare queue as well because we might start
                    // the consumer before the publisher, we want to make sure
                    // the queue exists before we try to consume messages from it.
                    channel.QueueDeclare(queue: "hello",
                            durable: false,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);

                    // Sinse server pushes messages asynchoriously
                    // we provide callback
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);
                    };

                    // Start consuming
                    channel.BasicConsume(
                        queue: "simple_queue",
                        autoAck: true,
                        consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }            
        }
    }
}
