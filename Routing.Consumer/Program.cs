using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Routing.Consumer
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
                    channel.ExchangeDeclare(
                        exchange: "direct_logs",
                        type: "direct");

                    // When we supply no parameters to QueueDeclare() we create a 
                    // non-durable, exclusive, autodelete queue with a auto-generated name.
                    var queueName = channel.QueueDeclare().QueueName;

                    // If no severety selected ends execution
                    if (args.Length < 1)
                    {
                        Console.Error.WriteLine("Usage: {0} [info] [warning] [error]",
                            Environment.GetCommandLineArgs()[0]);
                        Console.WriteLine(" Press [enter] to exit.");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    // We're going to create a new binding for each severity we're interested in.
                    foreach (var severity in args)
                    {
                        // A binding is a relationship between an exchange and a queue.
                        // This can be simply read as: the queue is interested in 
                        // messages from this exchange.

                        // Bindings can take an extra routingKey parameter.
                        // The meaning of a routingKey depends on the exchange type. 
                        // For direct exchange it acts like a filter for messages -
                        // a message goes to the queues whose routingKey exactly matches the 
                        // routing key of the message.

                        // Single queue could bind to several routingKeys (as here) and multiple
                        // queues could bind to the same routingKey.

                        channel.QueueBind(
                            queue: queueName,
                            exchange: "direct_logs",
                            routingKey: severity);
                    }
                    Console.WriteLine(" [*] Waiting for messages.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
                    };

                    channel.BasicConsume(
                        queue: queueName,
                        autoAck: true,
                        consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}
