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
        public static Dictionary<List<string>, (string ColumnName, bool IsShares, bool IsCashFlowStatement, bool IsBalanceSheet)> HTMLElementsOfInterest = new Dictionary<List<string>, (string ColumnName, bool IsShares, bool IsCashFlowStatement, bool IsBalanceSheet)>
{
    { new List<string> { "Net sales", "Net revenue" }, ("NetSales", false, false, false) },
    { new List<string> { "Cost of sales", "Cost of revenue" }, ("CostOfSales", false, false, false) },
    { new List<string> { "Gross margin", "Gross profit" }, ("GrossMargin", false, false, false) },
    { new List<string> { "Research and Development", "R&D" }, ("ResearchAndDevelopment", false, false, false) },
    { new List<string> { "Selling, general and administrative", "SG&A" }, ("SellingGeneralAndAdministrative", false, false, false) },
    { new List<string> { "Total operating expenses", "Operating expenses" }, ("TotalOperatingExpenses", false, false, false) },
    { new List<string> { "Operating income", "Operating profit" }, ("OperatingIncome", false, false, false) },
    { new List<string> { "Other income/(expense), net", "Other income" }, ("OtherIncomeNet", false, false, false) },
    { new List<string> { "Income before provision for income taxes", "Pre-tax income" }, ("IncomeBeforeProvisionForIncomeTaxes", false, false, false) },
    { new List<string> { "Net income", "Net profit" }, ("NetIncome", false, false, false) },
    { new List<string> { "Basic (in dollars per share)", "Basic earnings per share" }, ("SharesUsedInComputingEarningsPerShareBasic", false, false, false) },
    { new List<string> { "Diluted (in dollars per share)", "Diluted earnings per share" }, ("SharesUsedInComputingEarningsPerShareDiluted", false, false, false) },
    { new List<string> { "Basic (in shares)", "Basic share count" }, ("BasicInShares", true, false, false) },
    { new List<string> { "Diluted (in shares)", "Diluted share count" }, ("DilutedInShares", true, false, false) },
    { new List<string> { "Other comprehensive income (loss)", "Comprehensive income" }, ("OtherComprehensiveIncomeLoss", false, false, false) },
    { new List<string> { "Change in foreign currency translation", "Foreign currency translation adjustment" }, ("ChangeInForeignCurrencyTranslation", false, false, false) },
    { new List<string> { "Change in unrealized gains/losses on derivative instruments", "Derivative gains/losses" }, ("ChangeInUnrealizedGainsLossesOnDerivatives", false, false, false) },
    { new List<string> { "Change in unrealized gains/losses on marketable securities", "Marketable securities gains/losses" }, ("ChangeInUnrealizedGainsLossesOnMarketableSecurities", false, false, false) },
    { new List<string> { "Total other comprehensive income/(loss)", "Other comprehensive income" }, ("TotalOtherComprehensiveIncomeLoss", false, false, false) },
    { new List<string> { "Total comprehensive income", "Comprehensive income" }, ("TotalComprehensiveIncome", false, false, false) },
    { new List<string> { "Cash and cash equivalents", "Cash equivalents" }, ("CashAndCashEquivalents", false, false, true) },
    { new List<string> { "Short-term investments", "ST investments" }, ("ShortTermInvestments", false, false, true) },
    { new List<string> { "Accounts receivable, net", "Net receivables" }, ("AccountsReceivableNet", false, false, true) },
    { new List<string> { "Inventories", "Stock" }, ("Inventories", false, false, true) },
    { new List<string> { "Total current assets", "Current assets" }, ("TotalCurrentAssets", false, false, true) },
    { new List<string> { "Property and equipment, net", "P&E, net" }, ("PropertyAndEquipmentNet", false, false, true) },
    { new List<string> { "Goodwill" }, ("Goodwill", false, false, true) },
    { new List<string> { "Total assets" }, ("TotalAssets", false, false, true) },
    { new List<string> { "Accounts payable", "Payables" }, ("AccountsPayable", false, false, true) },
    { new List<string> { "Accrued expenses", "Accrued liabilities" }, ("AccruedExpenses", false, false, true) },
    { new List<string> { "Commercial paper" }, ("CommercialPaperHTML", false, false, true) },
    { new List<string> { "Deferred revenue", "Deferred income" }, ("DeferredRevenue", false, false, true) },
    { new List<string> { "Total current liabilities", "Current liabilities" }, ("TotalCurrentLiabilities", false, false, true) },
    { new List<string> { "Long-term debt", "LT debt" }, ("LongTermDebt", false, false, true) },
    { new List<string> { "Total liabilities", "Liabilities" }, ("TotalLiabilities", false, false, true) },
    { new List<string> { "Common stock and paid-in capital", "Paid-in capital" }, ("CommonStockAndPaidInCapital", false, false, true) },
    { new List<string> { "Retained earnings" }, ("RetainedEarnings", false, false, true) },
    { new List<string> { "Accumulated other comprehensive income/(loss)", "Accumulated OCI" }, ("AccumulatedOtherComprehensiveIncomeLoss", false, false, true) },
    { new List<string> { "Total stockholders' equity", "Stockholders' equity" }, ("TotalStockholdersEquity", false, false, true) },
    { new List<string> { "Total liabilities and stockholders' equity", "Liabilities and equity" }, ("TotalLiabilitiesAndStockholdersEquity", false, false, true) },
    { new List<string> { "Net cash from operations", "Cash from operating activities" }, ("NetCashFromOperations", false, true, false) },
    { new List<string> { "Net cash used in investing", "Cash used in investing activities" }, ("NetCashUsedInInvesting", false, true, false) },
    { new List<string> { "Net cash used in financing", "Cash used in financing activities" }, ("NetCashUsedInFinancing", false, true, false) },
    { new List<string> { "Net increase (decrease) in cash and cash equivalents", "Net increase in cash" }, ("NetIncreaseDecreaseInCashAndCashEquivalents", false, true, false) },
    { new List<string> { "Cash and cash equivalents, beginning of the period", "Cash at beginning of period" }, ("CashAndCashEquivalentsBeginningOfPeriod", false, true, false) },
    { new List<string> { "Cash and cash equivalents, end of the period", "Cash at end of period" }, ("CashAndCashEquivalentsEndOfPeriod", false, true, false) },
    { new List<string> { "Adjustments to reconcile net income to cash generated by operating activities", "Adjustments to net income" }, ("AdjustmentsToReconcileNetIncomeToCashGeneratedByOperatingActivities", false, true, false) },
    { new List<string> { "Depreciation and amortization", "Depreciation" }, ("DepreciationAndAmortization", false, true, false) },
    { new List<string> { "Stock-based compensation expense", "Stock compensation" }, ("StockBasedCompensationExpense", false, true, false) },
    { new List<string> { "Deferred income tax expense/(benefit)", "Deferred tax expense" }, ("DeferredIncomeTaxExpenseBenefit", false, true, false) },
    { new List<string> { "Changes in operating assets and liabilities", "Changes in assets and liabilities" }, ("ChangesInOperatingAssetsAndLiabilities", false, true, false) },
    { new List<string> { "Proceeds from issuance of common stock", "Proceeds from stock issuance" }, ("ProceedsFromIssuanceOfCommonStock", false, true, false) },
    { new List<string> { "Proceeds from sale of marketable securities", "Proceeds from securities sales" }, ("ProceedsFromSaleOfMarketableSecurities", false, true, false) },
    { new List<string> { "Payments for acquisition of property, plant, and equipment", "PPE acquisitions" }, ("PaymentsForAcquisitionOfPropertyPlantAndEquipment", false, true, false) },
    { new List<string> { "Cash paid for interest", "Interest paid" }, ("CashPaidForInterest", false, true, false) },
    { new List<string> { "Cash paid for income taxes", "Income taxes paid" }, ("CashPaidForIncomeTaxes", false, true, false) },
    { new List<string> { "Common stock issued", "Stock issued" }, ("CommonStockIssued", false, true, false) },
    { new List<string> { "Common stock repurchased", "Stock repurchased" }, ("CommonStockRepurchased", false, true, false) },
    { new List<string> { "Dividends declared" }, ("DividendsDeclared", false, true, false) }
};


        public class FinancialElement
        {
            public string Name { get; set; } = string.Empty;
            public string? Value { get; set; }
            public string? ContextRef { get; set; }
        }
    }
}
