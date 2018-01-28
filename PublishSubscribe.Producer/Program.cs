using System;
using System.Text;
using RabbitMQ.Client;

namespace PublishSubscribe.Producer
{
    // The core idea in the messaging model in RabbitMQ is that the 
    // producer never sends any messages directly to a queue. 
    // Actually, quite often the producer doesn't even know if a message 
    // will be delivered to any queue at all.
    // Instead, the producer can only send messages to an exchange.
    // Exchange on one side it receives messages from producers 
    // and the other side it pushes them to queues.

    // Exchange types: direct, topic, headers and fanout.
    // fanout exchange broadcasts all the messages it receives to all the queues it knows.

    internal class Program
    {
        private static void Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // The messages will be lost if no queue is bound to the exchange yet, 
                    // but that's okay for us; if no consumer is listening yet we can 
                    // safely discard the message.
                    channel.ExchangeDeclare(
                        exchange: "logs", 
                        type: "fanout");

                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(
                        exchange: "logs",
                        routingKey: "",
                        basicProperties: null,
                        body: body);

                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }
        }

        private static string GetMessage(string[] args)
        {
            return args.Length > 0 ? string.Join(" ", args) : "info: Hello World!";
        }
    }
}
