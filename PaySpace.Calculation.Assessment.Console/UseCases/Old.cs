using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
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
        private static Dictionary<int, CountryTaxData> CountryTaxDataCache;

        public async Task CalculateAsync()
        {
            // Load tax data into a static cache if not already loaded
            if (CountryTaxDataCache == null)
                CountryTaxDataCache = await LoadCountryTaxDataAsync();

            string taxCalculationQuery = @"
                SELECT tc.FkCountryId, tc.Income, tc.PkTaxCalculationId, c.TaxRegime
                FROM TaxCalculation tc WITH(NOLOCK)
                INNER JOIN Country c WITH(NOLOCK) ON tc.FkCountryId = c.PkCountryId";

            Stopwatch sw = Stopwatch.StartNew();

            using SqlConnection conn = new(ConnectionString);
            await conn.OpenAsync();
            var taxCalculations = (await conn.QueryAsync(taxCalculationQuery)).ToList();

            var updateList = new List<TaxCalculationUpdate>();

            // Process calculations in parallel
            await Parallel.ForEachAsync(taxCalculations, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, async (taxCalculation, _) =>
            {
                int countryId = taxCalculation.FkCountryId;
                string taxMethod = taxCalculation.TaxRegime;
                decimal income = taxCalculation.Income;
                decimal tax = 0m;

                // Access pre-loaded tax data from cache
                if (CountryTaxDataCache.TryGetValue(countryId, out var taxData))
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
                    updateList.Add(new TaxCalculationUpdate { TaxCalculationId = taxCalculation.PkTaxCalculationId, Tax = tax, NetPay = netPay });
                }
            });

            // Use TVP for bulk update
            await BulkUpdateTaxCalculationsAsync(conn, updateList);

            sw.Stop();
            Console.WriteLine($"{taxCalculations.Count} calculations completed in {sw.ElapsedMilliseconds}ms");
            await conn.CloseAsync();
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

        private async Task BulkUpdateTaxCalculationsAsync(SqlConnection conn, List<TaxCalculationUpdate> updates)
        {
            if (updates.Count == 0) return;

            // Create a DataTable to pass as a TVP
            var table = new DataTable();
            table.Columns.Add("TaxCalculationId", typeof(int));
            table.Columns.Add("CalculatedTax", typeof(decimal));
            table.Columns.Add("NetPay", typeof(decimal));

            foreach (var update in updates)
            {
                table.Rows.Add(update.TaxCalculationId, update.Tax, update.NetPay);
            }

            var bulkUpdateCommand = new SqlCommand("UpdateTaxCalculationsTVP", conn)
            {
                CommandType = CommandType.StoredProcedure
            };
            var tvpParam = bulkUpdateCommand.Parameters.AddWithValue("@TaxCalculations", table);
            tvpParam.SqlDbType = SqlDbType.Structured;
            tvpParam.TypeName = "dbo.TaxCalculationTVP"; // TVP type defined in SQL Server

            await bulkUpdateCommand.ExecuteNonQueryAsync();
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

        private class TaxCalculationUpdate
        {
            public int TaxCalculationId { get; set; }
            public decimal Tax { get; set; }
            public decimal NetPay { get; set; }
        }
    }
}
