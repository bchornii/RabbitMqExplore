using System;
using System.Text;
using RabbitMQ.Client;

namespace WorkQueue.Producer
{
    // Work Queue will be used to distribute 
    // time-consuming tasks among multiple workers.

    // The main idea behind Work Queues (aka.Task Queues) 
    // is to avoid doing a resource-intensive task immediately 
    // and having to wait for it to complete.

    // We encapsulate a task as a message and send it to a queue. 
    // A worker process running in the background will pop the tasks 
    // and eventually execute the job. 

    // When you run many workers the tasks will be shared between them.

    internal class Program
    {
        private static void Main(string[] args)
        {
            var factory = new ConnectionFactory {HostName = "localhost"};

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // To send, we must declare a queue for us to send to.
                    // Declaring a queue is idempotent - it will only be created 
                    // if it doesn't exist already.

                    // When RabbitMQ quits or crashes it will forget the queues 
                    // and messages unless you tell it not to. Two things are required 
                    // to make sure that messages aren't lost: we need to mark 
                    // both the queue and messages as durable.

                    channel.QueueDeclare(
                        queue: "durable_work_queue",
                        durable: true,      // 1) durable queue
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);
                    
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;   // 2) message persistance

                    channel.BasicPublish(
                        exchange: "",
                        routingKey: "durable_work_queue",
                        basicProperties: properties,
                        body: body);

                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }
        }

        private static string GetMessage(string[] args)
        {
            return args.Length > 0 ? string.Join(" ", args) : "Hello World!";
        }
    }
}
