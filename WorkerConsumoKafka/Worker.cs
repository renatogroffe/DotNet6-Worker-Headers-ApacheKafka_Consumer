using System.Text;
using Confluent.Kafka;

namespace WorkerConsumoKafka;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ParametrosExecucao _parametrosExecucao;
    private readonly IConsumer<Ignore, string> _consumer;

    public Worker(ILogger<Worker> logger,
        ParametrosExecucao parametrosExecucao)
    {
        _logger = logger;
        _parametrosExecucao = parametrosExecucao;

            _logger.LogInformation($"Bootstrap Servers = {parametrosExecucao.BootstrapServers}");
            _logger.LogInformation($"Topic = {parametrosExecucao.Topic}");
            _logger.LogInformation($"Group Id = {parametrosExecucao.GroupId}");

            _consumer = new ConsumerBuilder<Ignore, string>(
                new ConsumerConfig()
                {
                    BootstrapServers = parametrosExecucao.BootstrapServers,
                    GroupId = parametrosExecucao.GroupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                }).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Aguardando mensagens...");
        _consumer.Subscribe(_parametrosExecucao.Topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Run(() =>
            {
                var result = _consumer.Consume(stoppingToken);
                _logger.LogInformation(
                    $"[{_parametrosExecucao.GroupId} | Nova mensagem] " +
                    result.Message.Value);
                if (result.Message.Headers.Count > 0)
                {
                    _logger.LogInformation("Foram encontrados Headers!!!");
                    foreach (var header in result.Message.Headers)
                        _logger.LogInformation(
                            $"Header {header.Key} = {Encoding.ASCII.GetString(header.GetValueBytes())}");
                }
            });
        }
    }

    public override Task StopAsync(CancellationToken stoppingToken)
    {
        _consumer.Close();
        _logger.LogInformation(
            "Conexao com o Apache Kafka fechada!");
        return Task.CompletedTask;
    }
}