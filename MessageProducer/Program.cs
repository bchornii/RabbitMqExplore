using System;
using System.Text;
using RabbitMQ.Client;

namespace SimpleQueue.Producer
{
    // Queue is the place where messages are stored. It's essentially 
    // a large message buffer
    // Many producers can send messages to one queue
    // and many consumers can try receive data from one queue.
    internal class Program
    {
        private static void Main()
        {
            var factory = new ConnectionFactory {HostName = "localhost"};

            // The connection abstracts the socket connection, 
            // and takes care of protocol version negotiation and authentication            
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // To send, we must declare a queue for us to send to.
                    // Declaring a queue is idempotent - it will only be created 
                    // if it doesn't exist already.
                    channel.QueueDeclare(
                        queue: "simple_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    var message = "Hello world!";
                    var body = Encoding.UTF8.GetBytes(message);

                    // Then we can publish a message to the queue
                    channel.BasicPublish(
                        exchange: "",
                        routingKey: "simple_queue",
                        basicProperties: null,
                        body: body);

                    Console.WriteLine(" [x] Sent {0}", message);                    
                }

                // When the code above finishes running, 
                // the channel and the connection will be disposed.
                // Console.WriteLine(" Press [enter] to exit.");
                // Console.ReadLine();
            }
        }
    }    
}
