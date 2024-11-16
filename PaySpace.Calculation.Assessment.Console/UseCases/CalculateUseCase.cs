using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using Dapper;
using PaySpace.Calculation.Assessment.Console.Domain;
using Z.Dapper.Plus;

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
                // Load tax rates and brackets in bulk into memory for quick lookup
                var countryTaxData = await LoadCountryTaxDataAsync();

                string taxCalculationQuery = @"
                    SELECT tc.FkCountryId, tc.Income, tc.PkTaxCalculationId, c.TaxRegime
                    FROM TaxCalculation tc WITH(NOLOCK)
                    INNER JOIN Country c WITH(NOLOCK) ON tc.FkCountryId = c.PkCountryId";

                Stopwatch sw = Stopwatch.StartNew();

                // Fetch all tax calculations at once to minimize database calls
                using SqlConnection conn = new(ConnectionString);
                await conn.OpenAsync();
                var taxCalculations = (await conn.QueryAsync(taxCalculationQuery)).ToList();
                
                var updateList = new List<TaxCalculation>();

                // Process each calculation in parallel
                await Parallel.ForEachAsync(taxCalculations, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, async (taxCalculation, _) =>
                {
                    int countryId = taxCalculation.FkCountryId;
                    string taxMethod = taxCalculation.TaxRegime;
                    decimal income = taxCalculation.Income;
                    decimal tax = 0m;

                    // Use pre-loaded tax data for calculations
                    if (countryTaxData.TryGetValue(countryId, out var taxData))
                    {
                        switch (taxMethod)
                        {
                            case "PROG":
                                tax = CalculateProgressiveTax(taxData.TaxBrackets, income);
                                break;
                            case "PERC":
                                tax = CalculatePercentageTax(taxData.PercentageRate, income);
                                break;
                            case "FLAT":
                                tax = CalculateFlatTax(taxData.FlatRate, taxData.Threshold, income);
                                break;
                        }

                        decimal netPay = income - tax;
                        updateList.Add(new TaxCalculation { PkTaxCalculationId = taxCalculation.PkTaxCalculationId, CalculatedTax = tax, NetPay = netPay });
                    }
                });

                // Perform batch update
                await BulkUpdateTaxCalculationsAsync(conn, updateList);

                sw.Stop();
                System.Console.WriteLine($"{taxCalculations.Count} calculations completed in {sw.ElapsedMilliseconds}ms");
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

        private async Task BulkUpdateTaxCalculationsAsync(SqlConnection conn, List<TaxCalculation> updates)
        {
            if (updates.Count == 0) return;

    	    try
            {
                // await conn.ExecuteAsync(sb.ToString());
                conn.BulkUpdate($"UPDATE TaxCalculation SET CalculatedTax = @CalculatedTax, NetPay = @NetPay WHERE PkTaxCalculationId = @PkTaxCalculationId;",
                    updates);
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error updating tax calculations: {ex.Message}");
            }
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
