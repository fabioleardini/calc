using System.Data.SqlClient;
using System.Diagnostics;
using Dapper;

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
            var calculationCount = 0;

            try
            {
                string taxCalculationQuery = @"
                    SELECT tc.FkCountryId, tc.Income, tc.PkTaxCalculationId, c.TaxRegime
                    FROM TaxCalculation tc WITH(NOLOCK)
                    INNER JOIN Country c WITH(NOLOCK) ON tc.FkCountryId = c.PkCountryId
                    LEFT JOIN TaxBracket tb WITH(NOLOCK) ON tc.FkCountryId = tb.FkCountryId";

                Stopwatch sw = Stopwatch.StartNew();

                using SqlConnection conn = new(ConnectionString);
                await conn.OpenAsync(); // Ensure the connection is open

                var taxCalculations = await conn.QueryAsync(taxCalculationQuery);

                var updateTasks = new List<Task>();

                foreach (var taxCalculation in taxCalculations)
                {
                    calculationCount++;

                    int countryId = taxCalculation.FkCountryId;
                    string taxMethod = taxCalculation.TaxRegime;
                    decimal income = taxCalculation.Income;
                    var tax = 0m;
                    var netPay = 0m;

                    switch (taxMethod)
                    {
                        case "PROG":
                            tax = await CalculateProgressiveTaxAsync(conn, countryId, income);
                            break;

                        case "PERC":
                            tax = await CalculatePercentageTaxAsync(conn, countryId, income);
                            break;

                        case "FLAT":
                            tax = await CalculateFlatTaxAsync(conn, countryId, income);
                            break;
                    }

                    netPay = income - tax;

                    updateTasks.Add(UpdateTaxCalculationAsync(conn, taxCalculation.PkTaxCalculationId, tax, netPay));
                }

                await Task.WhenAll(updateTasks);

                sw.Stop();
                System.Console.WriteLine($"{calculationCount} calculations completed in {sw.ElapsedMilliseconds}ms");

                await conn.CloseAsync(); // Explicitly close the connection
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static async Task<decimal> CalculateProgressiveTaxAsync(SqlConnection conn, int countryId, decimal income)
        {
            string taxBracketLineQuery = @"
                SELECT LowerLimit, UpperLimit, Rate
                FROM TaxBracketLine l WITH(NOLOCK)
                INNER JOIN TaxBracket b WITH(NOLOCK) ON l.FkTaxBracketId = b.PkTaxBracketId
                WHERE b.FkCountryId = @CountryId
                ORDER BY OrderNumber";

            var taxBracketLines = await conn.QueryAsync(taxBracketLineQuery, new { CountryId = countryId });

            foreach (var taxBracketLine in taxBracketLines)
            {
                if (income > taxBracketLine.LowerLimit && income <= taxBracketLine.UpperLimit)
                {
                    return income * (taxBracketLine.Rate / 100);
                }
            }

            return 0m;
        }

        private static async Task<decimal> CalculatePercentageTaxAsync(SqlConnection conn, int countryId, decimal income)
        {
            string taxRateQuery = "SELECT Rate FROM TaxRate WHERE FkCountryId = @CountryId";
            var taxRate = await conn.QuerySingleOrDefaultAsync(taxRateQuery, new { CountryId = countryId });

            var percentage = 0m;

            if (taxRate != null && decimal.TryParse(taxRate.Rate.ToString(), out percentage))
            {
                return income * (percentage / 100);
            }

            return 0m;
        }

        private static async Task<decimal> CalculateFlatTaxAsync(SqlConnection conn, int countryId, decimal income)
        {
            string queryString = "SELECT Rate, RateCode FROM TaxRate WHERE FkCountryId = @CountryId";
            var taxRates = await conn.QueryAsync(queryString, new { CountryId = countryId });

            decimal flatRate = 0m;
            decimal minimumThreshold = 0m;

            foreach (var rate in taxRates)
            {
                if (rate.RateCode == "FLATRATE")
                {
                    flatRate = (decimal)rate.Rate;
                }
                else if (rate.RateCode == "THRES")
                {
                    minimumThreshold = (decimal)rate.Rate;
                }
            }

            return income > minimumThreshold ? flatRate : 0m;
        }

        private static async Task UpdateTaxCalculationAsync(SqlConnection conn, int taxCalculationId, decimal tax, decimal netPay)
        {
            string updateQuery = "UPDATE TaxCalculation SET CalculatedTax = @Tax, NetPay = @NetPay WHERE PkTaxCalculationId = @TaxCalculationId";
            await conn.ExecuteAsync(updateQuery, new { Tax = tax, NetPay = netPay, TaxCalculationId = taxCalculationId });
        }
    }
}