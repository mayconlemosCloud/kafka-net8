using Confluent.Kafka;

namespace KafkaProducer;

public class Program
{
    public static async Task Main(string[] args)
    {
        var configuracao = new ProducerConfig()
        {
            BootstrapServers = "127.0.0.1:9092",
        };

        using var producer = new ProducerBuilder<Null, string>(configuracao).Build();

        try
        {
            Console.WriteLine("Digite exit para finalizar");
            while (true)
            {
                Console.WriteLine("Digite a mensagem: ");
                string msg = Console.ReadLine();

                if (msg == "exit") break;

                var dr = await producer.ProduceAsync("test-topico", new Message<Null, string> { Value = msg });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }

            Console.WriteLine("Finalizado");
        }
        catch (ProduceException<Null, string> exception)
        {
            Console.WriteLine($"Delivery failed: {exception.Error.Reason}");
        }
    }
}