using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //await InitData();
            //await DemoIndexing();
            await DemoBulkSupport();
        }

        //private static async Task DemoBulkSupport()
        //{
        //    var cosmosService = new CosmosService(new CosmosClientOptions());
        //}

        private static async Task DemoIndexing()
        {
            var cosmosService = new CosmosService("demo-container", new CosmosClientOptions());
            var partitionKey = nameof(Transaction);
            var investorId = Guid.NewGuid().ToString();
            var transaction = GetTransactions(1, partitionKey, investorId).FirstOrDefault();

            var insertResponse = await cosmosService.InsertItemAsync(transaction, partitionKey);
            Console.WriteLine($"Created item id:{transaction.id}. Request Charge {insertResponse.RequestCharge} RUs.\n");

            var updateResponse = await cosmosService.UpsertItemAsync(transaction, partitionKey);
            Console.WriteLine($"Updated item id:{transaction.id}. Request Charge {updateResponse.RequestCharge} RUs.\n");

            var query = $"select * from c where c.id = '{transaction.id}'";
            var (item, requestCharge) = await cosmosService.ReadItemsByQueryAsync<Transaction>(new QueryDefinition(query), partitionKey);
            Console.WriteLine($"Read item id:{transaction.id}. Request Charge {requestCharge} RUs.\n");

            var deleteResponse = await cosmosService.DeleteItemAsync<Transaction>(transaction.id, partitionKey);
            Console.WriteLine($"Delete item id:{transaction.id}. Request Charge {deleteResponse.RequestCharge} RUs.\n");
        }

        private static async Task DemoBulkSupport()
        {
            var watchDemoAllowBulkExecutionFalse = System.Diagnostics.Stopwatch.StartNew();
            var rus1 = await DemoAllowBulkExecution(false, 1000);
            watchDemoAllowBulkExecutionFalse.Stop();

            var watchDemoAllowBulkExecutionTrue = System.Diagnostics.Stopwatch.StartNew();
            var rus2 = await DemoAllowBulkExecution(true, 1000);
            watchDemoAllowBulkExecutionTrue.Stop();

            Console.WriteLine($"AllowBulkExecution = False. Request Charge {rus1} RUs. TotalTimes: {watchDemoAllowBulkExecutionFalse.ElapsedMilliseconds}ms\n");
            Console.WriteLine($"AllowBulkExecution = True. Request Charge {rus2} RUs. TotalTimes: {watchDemoAllowBulkExecutionTrue.ElapsedMilliseconds}ms\n");

        }

        private static async Task<double> DemoAllowBulkExecution(bool allowBulkExecution, int maxTasks)
        {
            Console.WriteLine($"==========================Start AllowBulkExecution = {allowBulkExecution} with 5000 items, MaxBulkTasks = {maxTasks}.==========================\n");

            var cosmosOptions = new CosmosClientOptions();
            cosmosOptions.AllowBulkExecution = allowBulkExecution;
            //cosmosOptions.MaxRetryAttemptsOnRateLimitedRequests = 0;
            //cosmosOptions.MaxRetryWaitTimeOnRateLimitedRequests = 30s;
            var cosmosService = new CosmosService("demo-container", cosmosOptions);

            var partitionKey = nameof(Transaction);
            var transactions = GetTransactions(5000, partitionKey, string.Empty);
            var rus = await cosmosService.BulkInsertItemsAsync(transactions, partitionKey, maxTasks);

            Console.WriteLine($"==========================End AllowBulkExecution = {allowBulkExecution} with 5000 items, MaxBulkTasks = {maxTasks}.==========================\n");
            return rus;
        }

        public static async Task InitData()
        {
            Console.WriteLine("Generating Data");

            var cosmosService = new CosmosService("test-primary-key",new CosmosClientOptions());
            var countries = GetCountries();
            await cosmosService.BulkInsertItemsAsync(countries, nameof(Country));

            var investors = new List<Investor>();
            foreach(var country in countries)
            {
                var partitionKey = $"{nameof(Investor)}-{country.id}";
                var investorsByCountry = GetInvestors(1000, country, partitionKey);
                await cosmosService.BulkInsertItemsAsync(investorsByCountry, investorsByCountry.Select(x => x.PartitionKey).FirstOrDefault(), 100);
                investors.AddRange(investorsByCountry);
            }

            foreach (var investor in investors)
            {
                var partitionKey = $"{nameof(Transaction)}-{investor.id}";
                var transactions = GetTransactions(10, partitionKey, investor.id);
                await cosmosService.BulkInsertItemsAsync(transactions, partitionKey,100);
            }
        }

        public static List<Transaction> GetTransactions(int amount, string partitionKey, string investorId)
        {
            var random = new Random();
            var transactions = new List<Transaction>();
            for (int i = 0; i <= amount; i++)
            {
                var transaction = new Transaction
                {
                    id = Guid.NewGuid().ToString(),
                    Type = nameof(Transaction),
                    InvestorId = investorId,
                    HeaderId = Guid.NewGuid().ToString(),
                    Tenant = Guid.NewGuid().ToString(),
                    PartitionKey = partitionKey,
                    TransactionCurrencyId = Guid.NewGuid().ToString(),
                    TransactionTypeDescription = Guid.NewGuid().ToString(),
                    TransactionTypeChangeType = Guid.NewGuid().ToString(),
                    TransactionSecurityCode = Guid.NewGuid().ToString(),
                    TransactionTypeCode = Guid.NewGuid().ToString(),
                    TransactionTypeId = Guid.NewGuid().ToString(),
                    PositionId = Guid.NewGuid().ToString(),
                    HoldingId = Guid.NewGuid().ToString(),
                    AccountId = Guid.NewGuid().ToString(),
                    AskCurrencyCode = Guid.NewGuid().ToString(),
                    AccountCode = Guid.NewGuid().ToString(),
                    TransactionId = Guid.NewGuid().ToString(),
                    TransactionCategory = Guid.NewGuid().ToString(),
                    TransactionCategoryClass = Guid.NewGuid().ToString(),
                    TransactionSettlementCurrencyCode = Guid.NewGuid().ToString(),
                    SecurityId = Guid.NewGuid().ToString(),
                    SecurityCode = Guid.NewGuid().ToString(),
                    CurrencyId = Guid.NewGuid().ToString(),
                    CurrencyCode = Guid.NewGuid().ToString(),
                    SecurityTypeId = Guid.NewGuid().ToString(),
                    SecurityTypeCode = Guid.NewGuid().ToString(),
                    SourceId = Guid.NewGuid().ToString(),
                    UnitBalance = random.Next(0, 999909999),
                    RemainingPrincipalBalance = random.Next(0, 999909999),
                    BookValue = random.Next(0, 999909999),
                    AskBookValue = random.Next(0, 999909999),
                    NetBookValue = random.Next(0, 999909999),
                    AskNetBookValue = random.Next(0, 999909999),
                    AdjustedBookValue = random.Next(0, 999909999),
                    AskAdjustedBookValue = random.Next(0, 999909999),
                    AccountingCost = random.Next(0, 999909999),
                    AskAccountingCost = random.Next(0, 999909999),
                    AccrualForBookValue = random.Next(0, 999909999),
                    WeightedPriceDifferenceFromPar = random.Next(0, 999909999),
                    TransactionProceeds = random.Next(0, 999909999),
                    AskTransactionSettlementAmount = random.Next(0, 999909999),
                    AskTransactionAccruedOnSettle = random.Next(0, 999909999),
                    PriceToAskingRate = random.Next(0, 999909999),
                    AskingRate = random.Next(0, 999909999),
                    SettlementToAskingRate = random.Next(0, 999909999),
                    SettlementToAskingRateOnSettle = random.Next(0, 999909999),
                    Exposure = random.Next(0, 999909999),
                    AskExposure = random.Next(0, 999909999),
                    AskTransactionPortfolioAssetFlow = random.Next(0, 999909999),
                    AskTransactionSecurityAssetFlow = random.Next(0, 999909999),
                    AskNetPortfolioAssetFlow = random.Next(0, 999909999),
                    AskNetSecurityAssetFlow = random.Next(0, 999909999),
                    AverageCost = random.Next(0, 999909999),
                    AskAverageCost = random.Next(0, 999909999),
                    InternalAskAverageCost = random.Next(0, 999909999),
                    AskTransactionPriceGainLoss = random.Next(0, 999909999),
                    AskTransactionForexGainLoss = random.Next(0, 999909999),
                    AskTransactionIncomeGainLoss = random.Next(0, 999909999),
                    AskTransactionCapitalGainLoss = random.Next(0, 999909999),
                    AskForexGainLoss = random.Next(0, 999909999),
                    AskPriceGainLoss = random.Next(0, 999909999),
                    AskIncomeGainLoss = random.Next(0, 999909999),
                    AskCapitalGainLoss = random.Next(0, 999909999),
                    AskTransactionDeemCost = random.Next(0, 999909999),
                    AskRealizedGainLoss = random.Next(0, 999909999),
                    TransactionInterestIncome = random.Next(0, 999909999),
                    AskTransactionInterestIncome = random.Next(0, 999909999),
                    AskTransactionDividendIncome = random.Next(0, 999909999),
                    AskTransactionWithHoldingTax = random.Next(0, 999909999),
                    AskUnpaidInterest = random.Next(0, 999909999),
                    InterestIncome = random.Next(0, 999909999),
                    AskDividendIncome = random.Next(0, 999909999),
                    AskWithHoldingTax = random.Next(0, 999909999),
                    AskInterestIncome = random.Next(0, 999909999),
                    YieldOnBook = random.Next(0, 999909999),
                    TransactionYieldOnBook = random.Next(0, 999909999),
                    TransactionUnits = random.Next(0, 999909999),
                    TransactionUnitsFromTransaction = random.Next(0, 999909999),
                    TransactionSettlementAmount = random.Next(0, 999909999),
                    IsCash = true,
                    TransactionNumber = random.Next(0, 999909999),
                    TransactionSettlementRate = random.Next(0, 999909999),
                    TransactionPrice = random.Next(0, 999909999),
                    TransactionBookValue = random.Next(0, 999909999),
                    TransactionFees = random.Next(0, 999909999),
                    TransactionAccrued = random.Next(0, 999909999),
                    AskTransactionAccrued = random.Next(0, 999909999),
                    AskTransactionBookValue = random.Next(0, 999909999),
                    PreviousUnitBalance = random.Next(0, 999909999),
                    PreviousRemainingPrincipal = random.Next(0, 999909999),
                    PreviousBookValue = random.Next(0, 999909999),
                    PreviousAskBookValue = random.Next(0, 999909999),
                    PreviousAskAverageCost = random.Next(0, 999909999),
                    PreviousInternalAskAverageCost = random.Next(0, 999909999),
                    PreviousAverageCost = random.Next(0, 999909999),
                    PreviousYieldOnBook = random.Next(0, 999909999),
                    PreviousAskNetPortfolioAssetFlow = random.Next(0, 999909999),
                    PreviousAskNetSecurityAssetFlow = random.Next(0, 999909999),
                    LastBuyUnits = random.Next(0, 999909999),
                    LastSellUnits = random.Next(0, 999909999),
                    LastBuyUnitPrice = random.Next(0, 999909999),
                    LastSellUnitPrice = random.Next(0, 999909999),
                    Factor = random.Next(0, 999909999),
                    AskManagementFee = random.Next(0, 999909999),
                    AskCustfee = random.Next(0, 999909999),
                    AskCashdp = random.Next(0, 999909999),
                    AskCashwd = random.Next(0, 999909999),
                    AskCashxfrin = random.Next(0, 999909999),
                    AskCashfrout = random.Next(0, 999909999),
                    MarkToMarket = random.Next(0, 999909999),
                    AccrualForDiscountedSecurity = random.Next(0, 999909999),
                    AskAccrualForDiscountedSecurity = random.Next(0, 999909999),
                    AskSettlementAmountOnSettleDate = random.Next(0, 999909999),
                    YieldOnAdjustedBook = random.Next(0, 999909999),
                    AdjustedAverageCost = random.Next(0, 999909999),
                    AskAdjustedAverageCost = random.Next(0, 999909999),
                    AskAdjustedCapitalGainLoss = random.Next(0, 999909999),
                    AskAdjustedPriceGainLoss = random.Next(0, 999909999),
                    AskAdjustedForexGainLoss = random.Next(0, 999909999),
                    AskTransactionAdjustedDeemCost = random.Next(0, 999909999),
                    AskTransactionAdjustedCapitalGainLoss = random.Next(0, 999909999),
                    AskTransactionAdjustedPriceGainLoss = random.Next(0, 999909999),
                    AskTransactionAdjustedForexGainLoss = random.Next(0, 999909999),
                    AskCapitalGainIncome = random.Next(0, 999909999),
                    AskForeignIncome = random.Next(0, 999909999),
                    AskSecuxfrin = random.Next(0, 999909999),
                    AskSecuxfrout = random.Next(0, 999909999),
                    AskTransactionCapitalGainIncome = random.Next(0, 999909999),
                    AskTransactionForeignIncome = random.Next(0, 999909999),
                    AskSecudp = random.Next(0, 999909999),
                    AskSecuwd = random.Next(0, 999909999),
                    AskTransactionProceeds = random.Next(0, 999909999),
                    AskTransactionManagementFee = random.Next(0, 999909999),
                    AskTransactionCustfee = random.Next(0, 999909999),
                    AskMarketValueAdjustment = random.Next(0, 999909999),
                    RemainingCommitment = random.Next(0, 999909999),
                    MarketValue = random.Next(0, 999909999),
                    MarketValueBase = random.Next(0, 999909999),
                    FirstSettlementDate = DateTime.Now,
                    LastPositiveImpactDateOnAccountingCost = DateTime.Now,
                    TradeDate = DateTime.Now.AddDays(-i),
                    SettlementDate = DateTime.Now,
                    EffectiveDate = DateTime.Now,
                    LastBuyDate = DateTime.Now,
                    LastSellDate = DateTime.Now,
                    Born = DateTime.Now,
                };

                transactions.Add(transaction);
            }

            return transactions;

        }
        public static List<Investor> GetInvestors(int amount, Country country, string partitionKey)
        {
            var investors = new List<Investor>();

            for (int i = 0; i <= amount; i++)
            {
                var investor = new Investor
                {
                    id = Guid.NewGuid().ToString(),
                    Type = nameof(Investor),
                    CreatedDate = DateTime.Now.AddDays(-i),
                    CountryId = country.id,
                    Name = $"{nameof(Investor)}-{country.CountryCode}-{i + 1}",
                    PartitionKey = partitionKey,
                };
                investors.Add(investor);
            }

            return investors;
        }
        public static List<Country> GetCountries()
        {
            var countries = new List<Country>();
            var vn = new Country
            {
                id = "VN",
                Type = nameof(Country),
                CountryCode = "VN",
                CountryName = "Vietnam",
                PartitionKey = nameof(Country)
            };
            countries.Add(vn);


            var usa = new Country
            {
                id = "USA",
                Type = nameof(Country),
                CountryCode = "USA",
                CountryName = "United States",
                PartitionKey = nameof(Country)
            };
            countries.Add(usa);

            var CAN = new Country
            {
                id = "CAN",
                Type = nameof(Country),
                CountryCode = "CAN",
                CountryName = "Canada",
                PartitionKey = nameof(Country)
            };
            countries.Add(CAN);

            var China = new Country
            {
                id = "CHN",
                Type = nameof(Country),
                CountryCode = "CHN",
                CountryName = "China",
                PartitionKey = nameof(Country)
            };
            countries.Add(China);

            var Brazil = new Country
            {
                id = "BRA",
                Type = nameof(Country),
                CountryCode = "BRA",
                CountryName = "Brazil",
                PartitionKey = nameof(Country)
            };
            countries.Add(Brazil);


            return countries;
        }
    }
}
