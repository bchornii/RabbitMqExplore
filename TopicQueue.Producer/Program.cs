using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

/* 
 * In our logging system we might want to subscribe to not 
 * only logs based on severity, but also based on the 
 * source which emitted the log.
 * 
 * Messages sent to a topic exchange can't have an arbitrary 
 * routing_key - it must be a list of words, delimited by dots.
 * for ex.: "stock.usd.nyse", "nyse.vmw", "quick.orange.rabbit"
 * 
 * The logic behind the topic exchange is similar to a direct one - 
 * a message sent with a particular routing key will be delivered 
 * to all the queues that are bound with a matching binding key (routingKey parameter).
 * 
 * Two important special cases for binding keys:
 *  - * (star) can substitute for exactly one word
 *  - # (hash) can substitute for zero or more words
 * 
 * When a queue is bound with "#" (hash) binding key - it will 
 * receive all the messages, regardless of the routing key - 
 * like in fanout exchange.
 * 
 * When special characters "*" (star) and "#" (hash) aren't 
 * used in bindings, the topic exchange will behave just like 
 * a direct one.
 */

namespace TopicQueue.Producer
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

                    var routingKey = GetRoutingKey(args);
                    var message = GetMessage(args);

                    var body = Encoding.UTF8.GetBytes(message);

                    // Publish message to exhange
                    channel.BasicPublish(
                        exchange: "topic_logs",
                        routingKey: routingKey,
                        basicProperties: null,
                        body: body);

                    Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
                }
            }
        }

        private static string GetMessage(string[] args) => 
            args.Length > 1 ? string.Join(" ", args.Skip(1)) : "Hello World!";

        private static string GetRoutingKey(string[] args) => 
            args.Length > 0 ? args[0] : "anonymous.info";
    }
}
