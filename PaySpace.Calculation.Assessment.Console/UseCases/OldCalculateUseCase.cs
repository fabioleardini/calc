using System.Data.SqlClient;
using System.Diagnostics;
using Dapper;
using PaySpace.Calculation.Assessment.Console.Domain;

namespace PaySpace.Calculation.Assessment.Console.UseCases;

public interface IOldCalculateUseCase
{
    void Calculate();
}

public class OldCalculateUseCase : IOldCalculateUseCase
{
    public void Calculate()
    {
        var calculationCount = 0;
        string connectionString = "Data Source=localhost\\SQLEXPRESS;Initial Catalog=PaySpace;Timeout=180;MultipleActiveResultSets=true;User ID=sa;Password=Pass@word61197;";

        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            string taxCalculationQueries = "select * from TaxCalculation";
            SqlCommand taxCalcCommand = new SqlCommand(taxCalculationQueries, conn);
            conn.Open();
            SqlDataReader taxCalcReader = taxCalcCommand.ExecuteReader();
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                while (taxCalcReader.Read())
                {
                    calculationCount++;
                    var countryId = int.Parse(taxCalcReader["FkCountryId"].ToString());
                    var taxMethod = string.Empty;
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        string queryString = "select * from Country where pkCountryId = " + countryId;
                        SqlCommand command = new SqlCommand(queryString, connection);
                        connection.Open();
                        SqlDataReader reader = command.ExecuteReader();
                        try
                        {
                            reader.Read();
                            taxMethod = reader["TaxRegime"].ToString();
                        }
                        finally
                        {
                            reader.Close();
                        }
                    }

                    var income = decimal.Parse(taxCalcReader["Income"].ToString());
                    var tax = 0m;
                    var netPay = 0m;
                    switch (taxMethod)
                    {
                        case "PROG":
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                string queryString = "select * from TaxBracketLine l inner join TaxBracket b on l.FkTaxBracketId = b.PkTaxBracketId where fkCountryId = " + countryId + " order by OrderNumber";
                                SqlCommand command = new SqlCommand(queryString, connection);
                                connection.Open();
                                SqlDataReader reader = command.ExecuteReader();
                                try
                                {
                                    while (reader.Read())
                                    {
                                        if (income > decimal.Parse(reader["LowerLimit"].ToString()) && income <= decimal.Parse(reader["UpperLimit"].ToString()))
                                        {
                                            var rate = decimal.Parse(reader["Rate"].ToString());
                                            tax = income * (rate / 100);
                                            netPay = income - tax;
                                        }
                                    }
                                }
                                finally
                                {
                                    reader.Close();
                                }
                            }

                            break;

                        case "PERC":
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                string queryString = "select * from TaxRate where fkCountryId = " + countryId;
                                SqlCommand command = new SqlCommand(queryString, connection);
                                connection.Open();
                                SqlDataReader reader = command.ExecuteReader();
                                try
                                {
                                    reader.Read();
                                    var percentage = decimal.Parse(reader["Rate"].ToString());
                                    tax = income * (percentage / 100);
                                    netPay = income - tax;
                                }
                                finally
                                {
                                    reader.Close();
                                }
                            }

                            break;

                        case "FLAT":
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                string queryString = "select * from TaxRate where fkCountryId = " + countryId;
                                SqlCommand command = new SqlCommand(queryString, connection);
                                connection.Open();
                                SqlDataReader reader = command.ExecuteReader();
                                try
                                {
                                    var flatRate = 0m;
                                    var minimumThreshold = 0m;
                                    while (reader.Read())
                                    {
                                        if (reader["RateCode"].ToString() == "FLATRATE")
                                        {
                                            flatRate = decimal.Parse(reader["Rate"].ToString());
                                        }
                                        else if (reader["RateCode"].ToString() == "THRES")
                                        {
                                            minimumThreshold = decimal.Parse(reader["Rate"].ToString());
                                        }
                                    }

                                    if (income > minimumThreshold)
                                    {
                                        tax = flatRate;
                                        netPay = income - tax;
                                    }
                                    else
                                    {
                                        tax = 0m;
                                        netPay = income;
                                    }
                                }
                                finally
                                {
                                    reader.Close();
                                }
                            }

                            break;
                    }
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        string queryString = "update TaxCalculation set CalculatedTax = " + tax + ", NetPay = " + netPay + " where PkTaxCalculationId = " + taxCalcReader["PkTaxCalculationId"];
                        SqlCommand command = new SqlCommand(queryString, connection);
                        connection.Open();
                        command.ExecuteNonQuery();
                    }
                }
                sw.Stop();
                System.Console.WriteLine("OLD: " + calculationCount + " calculations completed in " + sw.ElapsedMilliseconds + "ms");
            }
            catch(Exception ex)
            {
                System.Console.WriteLine("Error: " + ex.Message);
            }
            finally
            {
                taxCalcReader.Close();
            }
        }
    }
}