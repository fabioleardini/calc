using System.Data.SqlClient;
using Dapper;
using PaySpace.Calculation.Assessment.Console.Domain;

namespace PaySpace.Calculation.Assessment.Console.Infrastructure;

public interface ITaxBracketLineRepositoryRepository
{
    Task<IEnumerable<TaxBracketLine>> GetTaxBracketLinesAsync(int countryId);
}

public class TaxBracketLineRepositoryRepository : ITaxBracketLineRepositoryRepository
{
    private const string ConnectionString = "Data Source=localhost\\SQLEXPRESS;Initial Catalog=PaySpace;Timeout=180;MultipleActiveResultSets=true;User ID=sa;Password=Pass@word61197;";
    
    public async Task<IEnumerable<TaxBracketLine>> GetTaxBracketLinesAsync(int countryId)
    {
        string taxBracketLineQuery = @"
            SELECT LowerLimit, UpperLimit, Rate
            FROM TaxBracketLine l WITH(NOLOCK)
            INNER JOIN TaxBracket b WITH(NOLOCK) ON l.FkTaxBracketId = b.PkTaxBracketId
            WHERE b.FkCountryId = @CountryId
            ORDER BY OrderNumber";

        using SqlConnection conn = new(ConnectionString);
        return await conn.QueryAsync<TaxBracketLine>(taxBracketLineQuery, new { CountryId = countryId });
    }
}