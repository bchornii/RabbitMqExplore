using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TopicQueue.Consumer
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
                    // Declare exchange
                    channel.ExchangeDeclare(
                        exchange: "topic_logs",
                        type: "topic");

                    var queueName = channel.QueueDeclare().QueueName;

                    // Create queue with auto-generated name, dureless,
                    // which will be deleted after consumer down
                    if (args.Length < 1)
                    {
                        Console.Error.WriteLine("Usage: {0} [binding_key...]",
                            Environment.GetCommandLineArgs()[0]);
                        Console.WriteLine(" Press [enter] to exit.");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var routingKey in args)
                    {
                        channel.QueueBind(
                            queue: queueName,
                            exchange: "topic_logs",
                            routingKey: routingKey);
                    }

                    Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
                    };

                    // Consume messages
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
