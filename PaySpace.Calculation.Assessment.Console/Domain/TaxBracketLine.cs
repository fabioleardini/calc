namespace PaySpace.Calculation.Assessment.Console.Domain;

public class TaxBracketLine
{
    public int OrderNumber { get; set; }
    public decimal LowerLimit { get; set; }
    public decimal UpperLimit { get; set; }
    public decimal Rate { get; set; }
    public int FkCountryId { get; set; }
}
