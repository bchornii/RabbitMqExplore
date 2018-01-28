using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PublishSubscribe.Consumer
{
    // We want to hear about all log messages, not just a subset of them.
    // We're also interested only in currently flowing messages not in the old ones.
    // To solve that we need two things:
    // - Whenever we connect to Rabbit we need a fresh, empty queue.
    //   To do this we could create a queue with a random name, or, 
    //   even better - let the server choose a random queue name for us.
    // - Once we disconnect the consumer the queue should be automatically deleted

    internal class Program
    {
        private static void Main()
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(
                        exchange: "logs", 
                        type: "fanout");

                    // When we supply no parameters to QueueDeclare() we create a 
                    // non-durable, exclusive, autodelete queue with a auto-generated name.
                    var queueName = channel.QueueDeclare().QueueName;

                    // We've already created a fanout exchange and a queue. 
                    // Now we need to tell the exchange to send messages to our queue. 
                    // That relationship between exchange and a queue is called a binding.
                    channel.QueueBind(
                        queue: queueName,
                        exchange: "logs",
                        routingKey: "");

                    Console.WriteLine(" [*] Waiting for logs.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] {0}", message);
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
