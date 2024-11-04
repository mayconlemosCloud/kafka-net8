using Confluent.Kafka;

namespace KafkaConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Conectando ao host 127.0.0.1:9092");
            var server = "127.0.0.1:9092";

            string topic;
            bool isTopicValid = false;

            var configuracao = new ConsumerConfig()
            {
                BootstrapServers = server,
                GroupId = "consumer-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Null, string>(configuracao).Build();

            while (!isTopicValid)
            {
                Console.WriteLine("Digite o tópico");
                topic = Console.ReadLine();
                try
                {
                    consumer.Subscribe(topic);
                    var result = consumer.Consume(TimeSpan.FromSeconds(3));
                    if (result != null)
                    {
                        Console.WriteLine($"Conectado ao tópico '{topic}' com sucesso.");
                        isTopicValid = true;
                    }
                    else
                    {
                        Console.WriteLine("Nenhuma mensagem encontrada ou tópico inexistente. Tente novamente.");
                        consumer.Unsubscribe();
                    }
                }
                catch (KafkaException ex)
                {
                    Console.WriteLine($"Erro ao tentar acessar o tópico: {ex.Message}. Tente novamente.");
                    consumer.Unsubscribe();
                }
            }

            Console.WriteLine("Consumindo...");
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (cr != null)
                    {
                        Console.WriteLine($"Received '{cr.Value}' from '{cr.TopicPartitionOffset}'");
                    }
                    else
                    {
                        // Informar que não há novas mensagens, mas continuar esperando
                        Console.WriteLine("Nenhuma nova mensagem disponível no momento. Continuando a escutar...");
                    }
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Error occurred while consuming: {ex.Error.Reason}");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
