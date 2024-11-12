CREATE TABLE [dbo].[TaxCalculation]
(
	[PkTaxCalculationId] INT NOT NULL PRIMARY KEY IDENTITY, 
    [FkCountryId] INT NOT NULL, 
    [Income] DECIMAL(31, 12) NOT NULL, 
    [CalculatedTax] DECIMAL(31, 12) NULL, 
    [NetPay] DECIMAL(31, 12) NULL, 
    CONSTRAINT [FK_TaxCalculation_Country] FOREIGN KEY ([FkCountryId]) REFERENCES [Country]([PkCountryId])
)
