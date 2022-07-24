using WorkerConsumoKafka;

Console.WriteLine(
    "Testando o consumo de mensagens com Apache Kafka");            

if (Environment.GetCommandLineArgs().Length != 4) // Desconsiderar primeiro parâmetro (Path do Executável)
{
    Console.WriteLine(
        "Informe 3 parametros: " +
        "no primeiro o Host Name + Porta do ambiente para testes do Kafka, " +
        "no segundo o Topic a ser utilizado no consumo das mensagens, " +
        "no terceiro o Group Id da aplicacao...");
    return;
}

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddSingleton<ParametrosExecucao>(
            new ParametrosExecucao()
            {
                BootstrapServers = args[0],
                Topic = args[1],
                GroupId = args[2]
            });

        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();