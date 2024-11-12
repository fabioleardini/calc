namespace PaySpace.Calculation.Assessment.Console.Domain;

public class TaxCalculation
{
    public int PkTaxCalculationId { get; set; }
    public int FkCountryId { get; set; }
    public string TaxRegime { get; set; }
    public decimal Income { get; set; }
    public decimal Tax { get; set; }
    public decimal NetPay { get; set; }
}
