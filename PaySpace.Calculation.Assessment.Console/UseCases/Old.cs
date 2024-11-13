using System.Data.SqlClient;
using System.Diagnostics;
using Dapper;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace PaySpace.Calculation.Assessment.Console.UseCases
{
    public interface ICalculateUseCase
    {
        Task CalculateAsync();
    }

    public class CalculateUseCase : ICalculateUseCase
    {
        private const string ConnectionString = "Data Source=localhost\\SQLEXPRESS;Initial Catalog=PaySpace;Timeout=180;MultipleActiveResultSets=true;User ID=sa;Password=Pass@word61197;";

        public async Task CalculateAsync()
        {
            try
            {
                // Load tax rates and brackets in bulk to reduce individual queries
                var countryTaxData = await LoadCountryTaxDataAsync();

                string taxCalculationQuery = @"
                    SELECT tc.FkCountryId, tc.Income, tc.PkTaxCalculationId, c.TaxRegime
                    FROM TaxCalculation tc WITH(NOLOCK)
                    INNER JOIN Country c WITH(NOLOCK) ON tc.FkCountryId = c.PkCountryId
                    LEFT JOIN TaxBracket tb WITH(NOLOCK) ON tc.FkCountryId = tb.FkCountryId";

                Stopwatch sw = Stopwatch.StartNew();

                using SqlConnection conn = new(ConnectionString);
                await conn.OpenAsync();

                var taxCalculations = await conn.QueryAsync(taxCalculationQuery);

                // Set up a buffer block for parallel processing
                var processingBlock = new ActionBlock<dynamic>(async taxCalculation =>
                {
                    int countryId = taxCalculation.FkCountryId;
                    string taxMethod = taxCalculation.TaxRegime;
                    decimal income = taxCalculation.Income;
                    decimal tax = 0m;

                    switch (taxMethod)
                    {
                        case "PROG":
                            tax = CalculateProgressiveTax(countryTaxData[countryId].TaxBrackets, income);
                            break;
                        case "PERC":
                            tax = CalculatePercentageTax(countryTaxData[countryId].PercentageRate, income);
                            break;
                        case "FLAT":
                            tax = CalculateFlatTax(countryTaxData[countryId].FlatRate, countryTaxData[countryId].Threshold, income);
                            break;
                    }

                    decimal netPay = income - tax;

                    await UpdateTaxCalculationAsync(conn, taxCalculation.PkTaxCalculationId, tax, netPay);
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount
                });

                // Post each tax calculation to the processing block
                foreach (var taxCalculation in taxCalculations)
                {
                    processingBlock.Post(taxCalculation);
                }

                // Signal completion and wait for processing to finish
                processingBlock.Complete();
                await processingBlock.Completion;

                sw.Stop();
                System.Console.WriteLine($"{taxCalculations.Count()} calculations completed in {sw.ElapsedMilliseconds}ms");

                await conn.CloseAsync();
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static decimal CalculateProgressiveTax(IEnumerable<TaxBracketLine> brackets, decimal income)
        {
            foreach (var bracket in brackets)
            {
                if (income > bracket.LowerLimit && income <= bracket.UpperLimit)
                {
                    return income * (bracket.Rate / 100);
                }
            }
            return 0m;
        }

        private static decimal CalculatePercentageTax(decimal percentage, decimal income)
        {
            return income * (percentage / 100);
        }

        private static decimal CalculateFlatTax(decimal flatRate, decimal threshold, decimal income)
        {
            return income > threshold ? flatRate : 0m;
        }

        private async Task UpdateTaxCalculationAsync(SqlConnection conn, int taxCalculationId, decimal tax, decimal netPay)
        {
            string updateQuery = "UPDATE TaxCalculation SET CalculatedTax = @Tax, NetPay = @NetPay WHERE PkTaxCalculationId = @TaxCalculationId";
            await conn.ExecuteAsync(updateQuery, new { Tax = tax, NetPay = netPay, TaxCalculationId = taxCalculationId });
        }

        private async Task<Dictionary<int, CountryTaxData>> LoadCountryTaxDataAsync()
        {
            using SqlConnection conn = new(ConnectionString);
            await conn.OpenAsync();

            // Load Tax Brackets
            var taxBracketsQuery = @"
                SELECT b.FkCountryId, l.LowerLimit, l.UpperLimit, l.Rate
                FROM TaxBracketLine l WITH(NOLOCK)
                INNER JOIN TaxBracket b WITH(NOLOCK) ON l.FkTaxBracketId = b.PkTaxBracketId";
            var taxBrackets = await conn.QueryAsync<TaxBracketLine>(taxBracketsQuery);

            // Load Tax Rates
            var taxRatesQuery = "SELECT FkCountryId, Rate, RateCode FROM TaxRate";
            var taxRates = await conn.QueryAsync(taxRatesQuery);

            // Organize data by country
            var countryTaxData = new Dictionary<int, CountryTaxData>();

            foreach (var taxRate in taxRates)
            {
                if (!countryTaxData.ContainsKey(taxRate.FkCountryId))
                    countryTaxData[taxRate.FkCountryId] = new CountryTaxData();

                if (taxRate.RateCode == "FLATRATE")
                    countryTaxData[taxRate.FkCountryId].FlatRate = (decimal)taxRate.Rate;
                else if (taxRate.RateCode == "THRES")
                    countryTaxData[taxRate.FkCountryId].Threshold = (decimal)taxRate.Rate;
                else
                    countryTaxData[taxRate.FkCountryId].PercentageRate = (decimal)taxRate.Rate;
            }

            foreach (var bracket in taxBrackets)
            {
                if (!countryTaxData.ContainsKey(bracket.FkCountryId))
                    countryTaxData[bracket.FkCountryId] = new CountryTaxData();

                countryTaxData[bracket.FkCountryId].TaxBrackets.Add(bracket);
            }

            await conn.CloseAsync();
            return countryTaxData;
        }

        private class CountryTaxData
        {
            public decimal PercentageRate { get; set; }
            public decimal FlatRate { get; set; }
            public decimal Threshold { get; set; }
            public List<TaxBracketLine> TaxBrackets { get; set; } = new();
        }

        private class TaxBracketLine
        {
            public int FkCountryId { get; set; }
            public decimal LowerLimit { get; set; }
            public decimal UpperLimit { get; set; }
            public decimal Rate { get; set; }
        }
    }
}
