-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

-- Reference table that contains the same data on all shards
IF OBJECT_ID('Regions', 'U') IS NULL
  BEGIN
    CREATE TABLE [Regions] (
      [RegionId] [INT]           NOT NULL,
      [Name]     [NVARCHAR](256) NOT NULL
        CONSTRAINT [PK_Regions_RegionId] PRIMARY KEY CLUSTERED (
          [RegionId] ASC
        )
    )

    INSERT INTO [Regions] ([RegionId], [Name]) VALUES (0, 'North America')
    INSERT INTO [Regions] ([RegionId], [Name]) VALUES (1, 'South America')
    INSERT INTO [Regions] ([RegionId], [Name]) VALUES (2, 'Europe')
    INSERT INTO [Regions] ([RegionId], [Name]) VALUES (3, 'Asia')
    INSERT INTO [Regions] ([RegionId], [Name]) VALUES (4, 'Africa')
    INSERT INTO [Regions] ([RegionId], [Name]) VALUES (5, 'Oceania')
  END
GO

-- Reference table that contains the same data on all shards
IF OBJECT_ID('Products', 'U') IS NULL
  BEGIN
    CREATE TABLE [Products] (
      [ProductId] [INT]           NOT NULL,
      [Name]      [NVARCHAR](256) NOT NULL
        CONSTRAINT [PK_Products_ProductId] PRIMARY KEY CLUSTERED (
          [ProductId] ASC
        )
    )

    INSERT INTO [Products] ([ProductId], [Name]) VALUES (0, 'Gizmos')
    INSERT INTO [Products] ([ProductId], [Name]) VALUES (1, 'Widgets')
  END
GO

-- Sharded table containing our sharding key (CustomerId)
IF OBJECT_ID('Customers', 'U') IS NULL
  CREATE TABLE [Customers] (
    [CustomerId] [INT]           NOT NULL, -- since we shard on this column, it cannot be an IDENTITY
    [Name]       [NVARCHAR](256) NOT NULL,
    [RegionId]   [INT]           NOT NULL
      CONSTRAINT [PK_Customer_CustomerId] PRIMARY KEY CLUSTERED (
        [CustomerID] ASC
      ),
    CONSTRAINT [FK_Customer_RegionId] FOREIGN KEY (
      [RegionId]
    ) REFERENCES [Regions] ([RegionId])
  )
GO

-- Sharded table that has a foreign key column containing our sharding key (CustomerId)
IF OBJECT_ID('Orders', 'U') IS NULL
  CREATE TABLE [Orders] (
    [CustomerId] [INT]      NOT NULL, -- since we shard on this column, it cannot be an IDENTITY
    [OrderId]    [INT]      NOT NULL IDENTITY (1, 1),
    [OrderDate]  [DATETIME] NOT NULL,
    [ProductId]  [INT]      NOT NULL
      CONSTRAINT [PK_Orders_CustomerId_OrderId] PRIMARY KEY CLUSTERED (
        [CustomerID] ASC,
        [OrderID] ASC
      ),
    CONSTRAINT [FK_Orders_CustomerId] FOREIGN KEY (
      [CustomerId]
    ) REFERENCES [Customers] ([CustomerId]),
    CONSTRAINT [FK_Orders_ProductId] FOREIGN KEY (
      [ProductId]
    ) REFERENCES [Products] ([ProductId])
  )
GO
