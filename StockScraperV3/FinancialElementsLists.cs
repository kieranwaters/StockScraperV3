using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HtmlAgilityPack;
using OpenQA.Selenium;


namespace DataElements
{
    public static class FinancialElementLists
    {
        public static HashSet<string> InstantDateElements { get; } = new HashSet<string>
        {
            "CashAndCashEquivalentsAtCarryingValue",
            "Assets", "Liabilities", "StockholdersEquity",
            "MarketableSecuritiesCurrent", "AccountsReceivableNetCurrent",
            "NontradeReceivablesCurrent", "InventoryNet", "OtherAssetsCurrent",
            "AssetsCurrent", "MarketableSecuritiesNoncurrent",
            "PropertyPlantAndEquipmentNet", "OtherAssetsNoncurrent",
            "AssetsNoncurrent", "AccountsPayableCurrent", "OtherLiabilitiesCurrent",
            "ContractWithCustomerLiabilityCurrent", "CommercialPaper",
            "LongTermDebtCurrent", "LiabilitiesCurrent",
            "LongTermDebtNoncurrent", "OtherLiabilitiesNoncurrent",
            "LiabilitiesNoncurrent", "Liabilities", "CommitmentsAndContingencies",
            "CommonStockSharesIssued", "CommonStockSharesOutstanding",
            "RetainedEarningsAccumulatedDeficit",
            "AccumulatedOtherComprehensiveIncomeLossNetOfTax",
            "StockholdersEquity", "LiabilitiesAndStockholdersEquity",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "EquitySecuritiesFvNiCost",
            "EquitySecuritiesFVNIAccumulatedGrossUnrealizedGainBeforeTax",
            "EquitySecuritiesFVNIAccumulatedGrossUnrealizedLossBeforeTax",
            "EquitySecuritiesFvNiCurrentAndNoncurrent",
            "AvailableForSaleDebtSecuritiesAmortizedCostBasis",
            "AvailableForSaleDebtSecuritiesAccumulatedGrossUnrealizedGainBeforeTax",
            "AvailableForSaleSecuritiesDebtSecurities",
            "AvailableForSaleDebtSecuritiesAccumulatedGrossUnrealizedLossBeforeTax",
            "CashCashEquivalentsAndMarketableSecuritiesCost",
            "CashEquivalentsAndMarketableSecuritiesAccumulatedGrossUnrealizedGainBeforeTax",
            "CashCashEquivalentsAndMarketableSecurities", "Cash"
        };

        public static HashSet<string> ElementsOfInterest { get; } = new HashSet<string>
        {
            "NetIncomeLossAvailableToCommonStockholdersBasicAbstract", "CostOfGoodsAndServicesSold",
            "RevenueFromContractWithCustomerExcludingAssessedTax", "GrossProfit",
            "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense",
            "OperatingExpenses", "NonoperatingIncomeExpense",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "IncomeTaxExpenseBenefit", "NetIncomeLoss",
            "EarningsPerShareBasic", "EarningsPerShareDiluted",
            "WeightedAverageNumberOfSharesOutstandingBasic",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "OtherComprehensiveIncomeLossForeignCurrencyTransactionAndTranslationAdjustmentNetOfTax",
            "OtherComprehensiveIncomeUnrealizedHoldingGainLossOnSecuritiesArisingDuringPeriodNetOfTax",
            "OtherComprehensiveIncomeLossAvailableForSaleSecuritiesAdjustmentNetOfTax",
            "OtherComprehensiveIncomeLossNetOfTaxPortionAttributableToParent",
            "ComprehensiveIncomeNetOfTax", "CashAndCashEquivalentsAtCarryingValue",
            "MarketableSecuritiesCurrent", "AccountsReceivableNetCurrent",
            "NontradeReceivablesCurrent", "InventoryNet", "OtherAssetsCurrent", "AssetsCurrent",
            "MarketableSecuritiesNoncurrent", "PropertyPlantAndEquipmentNet",
            "OtherAssetsNoncurrent", "AssetsNoncurrent", "Assets", "AccountsPayableCurrent",
            "OtherLiabilitiesCurrent", "ContractWithCustomerLiabilityCurrent",
            "CommercialPaper", "LongTermDebtCurrent", "LiabilitiesCurrent",
            "LongTermDebtNoncurrent", "OtherLiabilitiesNoncurrent",
            "LiabilitiesNoncurrent", "Liabilities", "CommitmentsAndContingencies",
            "CommonStockSharesIssued", "CommonStockSharesOutstanding",
            "RetainedEarningsAccumulatedDeficit",
            "AccumulatedOtherComprehensiveIncomeLossNetOfTax", "StockholdersEquity",
            "LiabilitiesAndStockholdersEquity", "StockIssuedDuringPeriodValueNewIssues",
            "Dividends", "StockRepurchasedAndRetiredDuringPeriodValue",
            "CommonStockDividendsPerShareDeclared",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "DepreciationDepletionAndAmortization", "ShareBasedCompensation",
            "OtherNoncashIncomeExpense", "IncreaseDecreaseInAccountsReceivable",
            "IncreaseDecreaseInInventories", "IncreaseDecreaseInOtherOperatingAssets",
            "IncreaseDecreaseInAccountsPayable",
            "IncreaseDecreaseInOtherOperatingLiabilities",
            "NetCashProvidedByUsedInOperatingActivities",
            "PaymentsToAcquireAvailableForSaleSecuritiesDebt",
            "ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities",
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsForProceedsFromOtherInvestingActivities",
            "NetCashProvidedByUsedInInvestingActivities",
            "PaymentsOfDividends", "PaymentsForRepurchaseOfCommonStock",
            "RepaymentsOfLongTermDebt", "ProceedsFromRepaymentsOfCommercialPaper",
            "ProceedsFromPaymentsForOtherFinancingActivities",
            "NetCashProvidedByUsedInFinancingActivities", "IncomeTaxesPaidNet",
            "EquitySecuritiesFvNiCost",
            "EquitySecuritiesFVNIAccumulatedGrossUnrealizedGainBeforeTax",
            "EquitySecuritiesFVNIAccumulatedGrossUnrealizedLossBeforeTax",
            "EquitySecuritiesFvNiCurrentAndNoncurrent",
            "AvailableForSaleDebtSecuritiesAmortizedCostBasis",
            "AvailableForSaleDebtSecuritiesAccumulatedGrossUnrealizedGainBeforeTax",
            "AvailableForSaleSecuritiesDebtSecurities",
            "AvailableForSaleDebtSecuritiesAccumulatedGrossUnrealizedLossBeforeTax",
            "CashCashEquivalentsAndMarketableSecuritiesCost",
            "CashEquivalentsAndMarketableSecuritiesAccumulatedGrossUnrealizedGainBeforeTax",
            "CashCashEquivalentsAndMarketableSecurities", "Cash"
        };
        public static bool IsElementPresent(IWebDriver driver, By by)
        {
            try
            {
                driver.FindElement(by);
                return true;
            }
            catch (NoSuchElementException)
            {
                return false;
            }
        }
        public static Dictionary<string, (string ColumnName, bool IsShares, bool IsCashFlowStatement, bool IsBalanceSheet)> HTMLElementsOfInterest = new Dictionary<string, (string ColumnName, bool IsShares, bool IsCashFlowStatement, bool IsBalanceSheet)>

    {
        {"Net sale", ("NetSales", false, false, false)},
        {"Cost of sales", ("CostOfSales", false, false, false)},
        {"Gross margin", ("GrossMargin", false, false, false)},
        {"Research and Development", ("ResearchAndDevelopment", false, false, false)},
        {"Selling, general and administrative", ("SellingGeneralAndAdministrative", false, false, false)  },
        {"Total operating expenses", ("TotalOperatingExpenses", false, false, false)},
        {"Operating income", ("OperatingIncome", false, false, false)},
        {"Other income/(expense), net", ("OtherIncomeNet", false, false, false)},
        {"Income before provision for income taxes", ("IncomeBeforeProvisionForIncomeTaxes", false, false, false)},
        {"Net income", ("NetIncome", false, false, false)},
        {"Basic (in dollars per share)", ("SharesUsedInComputingEarningsPerShareBasic", false, false, false)},
        {"Diluted (in dollars per share)", ("SharesUsedInComputingEarningsPerShareDiluted", false, false, false)},
        {"Basic (in shares)", ("BasicInShares", true, false, false)},
        {"Diluted (in shares)", ("DilutedInShares", true, false, false)},
        {"Other comprehensive income (loss)", ("OtherComprehensiveIncomeLoss", false, false, false)},
        {"Change in foreign currency translation", ("ChangeInForeignCurrencyTranslation", false, false, false)},
        {"Change in unrealized gains/losses on derivative instruments", ("ChangeInUnrealizedGainsLossesOnDerivatives", false, false, false)},
        {"Change in unrealized gains/losses on marketable securities", ("ChangeInUnrealizedGainsLossesOnMarketableSecurities", false, false,false)},
        {"Total other comprehensive income/(loss)", ("TotalOtherComprehensiveIncomeLoss", false, false, false)},
        {"Total comprehensive income", ("TotalComprehensiveIncome", false, false, false)},
        {"Cash and cash equivalents", ("CashAndCashEquivalents", false, false, true)},
        {"Short-term investments", ("ShortTermInvestments", false, false, true)},
        {"Accounts receivable, net", ("AccountsReceivableNet", false, false, true)},
        {"Inventories", ("Inventories", false, false, true)},
        {"Total current assets", ("TotalCurrentAssets", false, false, true)},
        {"Property and equipment, net", ("PropertyAndEquipmentNet", false, false, true)},
        {"Goodwill", ("Goodwill", false, false, true)},
        {"Total assets",    ("TotalAssets", false, false, true)},
        {"Accounts payable", ("AccountsPayable", false, false, true)},
        {"Accrued expenses", ("AccruedExpenses", false, false, true)},
        {"Commercial paper", ("CommercialPaperHTML", false, false, true)},
        {"Deferred revenue", ("DeferredRevenue", false, false, true)},
        {"Total current liabilities", ("TotalCurrentLiabilities", false, false, true)},
        {"Long-term debt", ("LongTermDebt", false, false, true)},
        {"Total liabilities", ("TotalLiabilities", false, false, true)},
        {"Common stock and paid-in capital", ("CommonStockAndPaidInCapital", false, false, true)},
        {"Retained earnings", ("RetainedEarnings", false, false, true)},
        {"Accumulated other comprehensive income/(loss)", ("AccumulatedOtherComprehensiveIncomeLoss", false, false, true)},
        {"Total stockholders' equity", ("TotalStockholdersEquity", false, false, true)},
        {"Total liabilities and stockholders' equity", ("TotalLiabilitiesAndStockholdersEquity", false, false, true)},
        {"Net cash from operations", ("NetCashFromOperations", false, true, false)},
        {"Net cash used in investing", ("NetCashUsedInInvesting", false, true, false)},
        {"Net cash used in financing", ("NetCashUsedInFinancing", false, true, false)},
        {"Net increase (decrease) in cash and cash equivalents", ("NetIncreaseDecreaseInCashAndCashEquivalents", false, true, false)},
        {"Cash and cash equivalents, beginning of the period", ("CashAndCashEquivalentsBeginningOfPeriod", false, true, false)},
        {"Cash and cash equivalents, end of the period", ("CashAndCashEquivalentsEndOfPeriod", false, true, false)},
        {"Adjustments to reconcile net income to cash generated by operating activities", ("AdjustmentsToReconcileNetIncomeToCashGeneratedByOperatingActivities", false, true, false)},
        {"Depreciation and amortization", ("DepreciationAndAmortization", false, true, false)},
        {"Stock-based compensation expense", ("StockBasedCompensationExpense", false, true, false)},
        {"Deferred income tax expense/(benefit)", ("DeferredIncomeTaxExpenseBenefit", false, true, false)},
        {"Changes in operating assets and liabilities", ("ChangesInOperatingAssetsAndLiabilities", false, true, false)},
        {"Proceeds from issuance of common stock", ("ProceedsFromIssuanceOfCommonStock", false, true, false)},
        {"Proceeds from sale of marketable securities", ("ProceedsFromSaleOfMarketableSecurities", false, true, false)},
        {"Payments for acquisition of property, plant, and equipment", ("PaymentsForAcquisitionOfPropertyPlantAndEquipment", false, true, false)},
        {"Cash paid for interest", ("CashPaidForInterest", false, true, false)},
        {"Cash paid for income taxes", ("CashPaidForIncomeTaxes", false, true, false)},
        {"Common stock issued", ("CommonStockIssued", false, true, false)},
        {"Common stock repurchased", ("CommonStockRepurchased", false, true, false)},
        {"Dividends declared", ("DividendsDeclared", false, true, false)}
    };

        public class FinancialElement
        {
            public string Name { get; set; } = string.Empty;
            public string? Value { get; set; }
            public string? ContextRef { get; set; }
        }
    }
}
