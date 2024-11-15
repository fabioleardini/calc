﻿using PaySpace.Calculation.Assessment.Console.UseCases;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using PaySpace.Calculation.Assessment.Console.Infrastructure;

var host = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services
                        .AddTransient<ICalculateUseCase, CalculateUseCase>()
                        .AddTransient<IOldCalculateUseCase, OldCalculateUseCase>()
                        .AddTransient<ITaxCalculationRepository, TaxCalculationRepository>()
                        .AddTransient<ITaxBracketLineRepositoryRepository, TaxBracketLineRepositoryRepository>();
                })
                .Build();

Console.WriteLine("***********************************************");
Console.WriteLine("*                   Welcome                   *");
Console.WriteLine("***********************************************");
Console.WriteLine("Calculating....");

host.Services.GetRequiredService<ICalculateUseCase>().CalculateAsync();
host.Services.GetRequiredService<IOldCalculateUseCase>().Calculate();
