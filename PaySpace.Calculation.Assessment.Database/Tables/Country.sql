CREATE TABLE [dbo].[Country]
(
	[PkCountryId] INT NOT NULL PRIMARY KEY IDENTITY, 
    [Description] VARCHAR(50) NOT NULL, 
    [Code] VARCHAR(3) NOT NULL, 
    [TaxRegime] VARCHAR(4) NOT NULL
)
