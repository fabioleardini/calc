namespace PaySpace.Calculation.Assessment.Console.Domain;

public class TaxRate
{
    public decimal Rate { get; set; }
    public string RateCode { get; set; }
    public int FkCountryId { get; set; }
}
