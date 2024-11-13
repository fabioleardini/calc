using System.Data.SqlClient;
using Dapper;
using PaySpace.Calculation.Assessment.Console.Domain;

namespace PaySpace.Calculation.Assessment.Console.Infrastructure
{
    public interface ITaxCalculationRepository
    {
        Task<IEnumerable<TaxCalculation>> GetTaxCalculationsAsync();
    }

    public class TaxCalculationRepository : ITaxCalculationRepository
    {
        private const string ConnectionString = "Data Source=localhost\\SQLEXPRESS;Initial Catalog=PaySpace;Timeout=180;MultipleActiveResultSets=true;User ID=sa;Password=Pass@word61197;";

        public async Task<IEnumerable<TaxCalculation>> GetTaxCalculationsAsync()
        {
            string taxCalculationQuery = @"
                SELECT tc.FkCountryId, tc.Income, tc.PkTaxCalculationId, c.TaxRegime
                FROM TaxCalculation tc WITH(NOLOCK)
                INNER JOIN Country c WITH(NOLOCK) ON tc.FkCountryId = c.PkCountryId
                LEFT JOIN TaxBracket tb WITH(NOLOCK) ON tc.FkCountryId = tb.FkCountryId";

            using SqlConnection conn = new(ConnectionString);

            return await conn.QueryAsync<TaxCalculation>(taxCalculationQuery);
        }
    }
}