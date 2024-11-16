namespace PaySpace.Calculation.Assessment.Console.Domain
{
    public class TaxCalculationUpdate
    {
        public int PkTaxCalculationId { get; set; }
        public decimal CalculatedTax { get; set; }
        public decimal NetPay { get; set; }
    }
}
