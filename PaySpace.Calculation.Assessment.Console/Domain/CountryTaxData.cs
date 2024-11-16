namespace PaySpace.Calculation.Assessment.Console.Domain
{
    public class CountryTaxData
    {
        public decimal PercentageRate { get; set; }
        public decimal FlatRate { get; set; }
        public decimal Threshold { get; set; }
        public List<TaxBracketLine> TaxBrackets { get; set; } = new();
    }
}
