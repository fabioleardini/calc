CREATE TABLE [dbo].[TaxBracket]
(
	[PkTaxBracketId] INT NOT NULL PRIMARY KEY IDENTITY, 
    [FkCountryId] INT NOT NULL, 
    [Code] VARCHAR(10) NOT NULL, 
    CONSTRAINT [FK_TaxBracket_Country] FOREIGN KEY ([FkCountryId]) REFERENCES [Country]([PkCountryId])
)
