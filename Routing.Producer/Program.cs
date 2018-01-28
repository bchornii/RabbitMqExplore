using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace Routing.Producer
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
                    // The messages will be lost if no queue is bound to the exchange yet, 
                    // but that's okay for us; if no consumer is listening yet we can 
                    // safely discard the message.
                    channel.ExchangeDeclare(
                        exchange: "direct_logs", 
                        type: "direct");

                    var severity = GetSeverity(args);
                    var message = GetMessage(args);

                    var body = Encoding.UTF8.GetBytes(message);

                    // To simplify things we will assume that 'severity' can be 
                    // one of 'info', 'warning', 'error'.
                    channel.BasicPublish(
                        exchange: "direct_logs",
                        routingKey: severity,
                        basicProperties: null,
                        body: body);

                    Console.WriteLine(" [x] Sent '{0}':'{1}'", severity, message);
                }
            }
        }

        private static string GetSeverity(string[] args) =>
            args.Length > 0 ? args[0] : "info";

        private static string GetMessage(string[] args) =>
            args.Length > 1 ? string.Join(" ", args.Skip(1)) : "Hello World!";
    }
}
