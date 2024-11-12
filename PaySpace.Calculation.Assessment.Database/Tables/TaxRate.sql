CREATE TABLE [dbo].[TaxRate]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY, 
    [FkCountryId] INT NOT NULL,
    [RateCode] VARCHAR(10) NOT NULL, 
    [Rate] DECIMAL(31, 12) NOT NULL, 
    CONSTRAINT [FK_TaxRate_Country] FOREIGN KEY ([FkCountryId]) REFERENCES [Country]([PkCountryId])
)
