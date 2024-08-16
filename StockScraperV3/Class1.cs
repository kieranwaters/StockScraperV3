using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using HtmlAgilityPack;

namespace AppleFinancialScraper;

class Program
{
    private static readonly string connectionString = "Server=LAPTOP-871MLHAT\\sqlexpress;Database=StockDataScraperDatabase;Integrated Security=True;";

    static async Task Main(string[] args)
    {
        var companyCIK = "320193";

        var filingUrls = (await GetFilingUrlsForLast10Years(companyCIK, "10-Q"))
            .Concat(await GetFilingUrlsForLast10Years(companyCIK, "10-K"))
            .ToList();

        if (!filingUrls.Any())
        {
            Console.WriteLine("No 10-Q or 10-K URLs found for the last 10 years.");
            return;
        }

        foreach (var url in filingUrls)
        {
            bool isAnnualReport = url.Contains("10-K");
            Console.WriteLine($"Processing URL: {url}, IsAnnualReport: {isAnnualReport}");

            string xbrlUrl = await GetXbrlUrl(url);
            if (!string.IsNullOrEmpty(xbrlUrl))
            {
                await DownloadAndParseXbrlData(xbrlUrl, isAnnualReport);
            }
            else
            {
                Console.WriteLine("No XBRL URL found. Attempting to parse HTML directly.");
                string htmlContent = await GetHtmlContent(url);
                ParseHtmlForElementsOfInterest(htmlContent, isAnnualReport);
            }
        }

        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    static async Task<List<string>> GetFilingUrlsForLast10Years(string companyCIK, string filingType)
    {
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
        string url = $"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={companyCIK}&type={filingType}&count=100&output=atom";

        string response = await client.GetStringAsync(url);
        XNamespace atom = "http://www.w3.org/2005/Atom";

        var urls = XDocument.Parse(response)
            .Descendants(atom + "entry")
            .Where(entry => DateTime.Parse(entry.Element(atom + "updated")?.Value ?? DateTime.MinValue.ToString()).Year >= DateTime.Now.Year - 10)
            .Select(entry => entry.Element(atom + "link")?.Attribute("href")?.Value)
            .ToList();

        Console.WriteLine($"{urls.Count} {filingType} URLs found.");
        return urls;
    }

    static async Task<string?> GetXbrlUrl(string filingUrl)
    {
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
        string response = await client.GetStringAsync(filingUrl);

        var doc = new HtmlDocument();
        doc.LoadHtml(response);

        var xbrlNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '_xbrl.xml') or contains(@href, '_htm.xml')]");
        if (xbrlNode != null)
        {
            string xbrlUrl = $"https://www.sec.gov{xbrlNode.GetAttributeValue("href", string.Empty)}";
            Console.WriteLine($"Direct XBRL URL found: {xbrlUrl}");
            return xbrlUrl;
        }

        var txtNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '.txt')]");
        if (txtNode != null)
        {
            string txtUrl = $"https://www.sec.gov{txtNode.GetAttributeValue("href", string.Empty)}";
            Console.WriteLine($"TXT file URL found: {txtUrl}");
            return txtUrl;
        }

        Console.WriteLine("XBRL content not found.");
        return null;
    }

    static async Task DownloadAndParseXbrlData(string filingUrl, bool isAnnualReport)
    {
        string content = filingUrl switch
        {
            var url when url.EndsWith("_htm.xml") || url.EndsWith("_xbrl.htm") || url.EndsWith(".xml") => await GetXbrlFromHtml(url),
            var url when url.EndsWith(".htm") => await GetHtmlContent(url),
            var url when url.EndsWith(".txt") => await GetEmbeddedXbrlContent(url),
            _ => string.Empty
        };

        if (string.IsNullOrEmpty(content))
        {
            Console.WriteLine("Content is null or not found.");
            return;
        }

        Console.WriteLine($"Content length: {content.Length}");
        Console.WriteLine($"First 200 characters:\n{content[..Math.Min(content.Length, 200)]}");

        if (filingUrl.EndsWith(".xml") || filingUrl.EndsWith("_htm.xml"))
        {
            ParseXbrlContent(content, isAnnualReport);
        }
        else
        {
            ParseHtmlForElementsOfInterest(content, isAnnualReport);
        }
    }

    static async Task<string> GetXbrlFromHtml(string url)
    {
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
        return await client.GetStringAsync(url);
    }

    static async Task<string> GetHtmlContent(string url)
    {
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (compatible; AcmeInc/1.0)");
        return await client.GetStringAsync(url);
    }

    static async Task<string> GetEmbeddedXbrlContent(string filingUrl)
    {
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
        string txtContent = await client.GetStringAsync(filingUrl);

        Console.WriteLine("Extracting embedded XBRL from txt file. Content preview:");
        Console.WriteLine(txtContent[..Math.Min(500, txtContent.Length)]);

        return ExtractEmbeddedXbrlFromTxt(txtContent);
    }

    private static string ExtractEmbeddedXbrlFromTxt(string txtContent)
    {
        var possibleXbrlTags = new[] { "<XBRL>", "</XBRL>", "<XBRLDocument>", "</XBRLDocument>", "<xbrli:xbrl>", "</xbrli:xbrl>" };

        foreach (var tag in possibleXbrlTags)
        {
            int xbrlStartIndex = txtContent.IndexOf(tag, StringComparison.OrdinalIgnoreCase);
            if (xbrlStartIndex != -1)
            {
                int xbrlEndIndex = txtContent.IndexOf(tag.Replace("<", "</"), StringComparison.OrdinalIgnoreCase) + tag.Replace("<", "</").Length;
                if (xbrlEndIndex != -1)
                {
                    Console.WriteLine("XBRL content found within txt file.");
                    return txtContent.Substring(xbrlStartIndex, xbrlEndIndex - xbrlStartIndex);
                }
            }
        }

        Console.WriteLine("XBRL content not found within the txt file.");
        return string.Empty;
    }

    static void ParseXbrlContent(string xbrlContent, bool isAnnualReport)
    {
        XDocument xDocument;
        try
        {
            xDocument = XDocument.Parse(xbrlContent);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing XBRL content: {ex.Message}");
            return;
        }

        Console.WriteLine("XBRL content parsed successfully.");

        var elementsOfInterest = GetElementsOfInterest();
        var contexts = xDocument.Descendants().Where(e => e.Name.LocalName == "context").ToDictionary(e => e.Attribute("id")?.Value, e => e);
        var elements = xDocument.Descendants()
            .Where(e => elementsOfInterest.Contains(e.Name.LocalName))
            .Select(e => new FinancialElement
            {
                Name = e.Name.LocalName,
                ContextRef = e.Attribute("contextRef")?.Value,
                Value = e.Value?.Trim()
            })
            .Where(e => e.ContextRef is not null && !string.IsNullOrEmpty(e.Value))
            .ToList();

        if (elements.Any())
        {
            foreach (var element in elements.Where(e => IsRelevantPeriod(contexts[e.ContextRef], isAnnualReport)))
            {
                Console.WriteLine($"Processing element: {element.Name} with value: {element.Value}");
                SaveToDatabase(element.Name, element.Value, contexts[element.ContextRef], elementsOfInterest.ToArray(), elements, isAnnualReport);
            }
        }
        else
        {
            Console.WriteLine("No elements of interest found in the document.");
        }
    }

    static void ParseHtmlForElementsOfInterest(string htmlContent, bool isAnnualReport)
    {
        var htmlDocument = new HtmlDocument();
        htmlDocument.LoadHtml(htmlContent);

        var elementsOfInterest = new Dictionary<string, string>
        {
            { "Net sales", "NetSales" },
            { "Cost of sales", "CostOfSales" },
            { "Gross margin", "GrossMargin" },
            { "Operating income", "OperatingIncome" },
            { "Research and development", "ResearchAndDevelopment" },
            { "Selling, general and administrative", "SellingGeneralAndAdministrative" },
            { "Total operating expenses", "TotalOperatingExpenses" },
            { "Income before provision for income taxes", "IncomeBeforeProvisionForIncomeTaxes" },
            { "Provision for income taxes", "ProvisionForIncomeTaxes" },
            { "Net income", "NetIncome" },
            { "Earnings per share: Basic", "EarningsPerShareBasic" },
            { "Earnings per share: Diluted", "EarningsPerShareDiluted" },
            { "Shares used in computing earnings per share: Basic", "SharesUsedInComputingEarningsPerShareBasic" },
            { "Shares used in computing earnings per share: Diluted", "SharesUsedInComputingEarningsPerShareDiluted" }
        };

        var table = htmlDocument.DocumentNode.SelectSingleNode("//table[contains(@style, 'BORDER-COLLAPSE')]");

        if (table != null)
        {
            Console.WriteLine("Table found in the HTML document.");

            foreach (var row in table.SelectNodes(".//tr"))
            {
                var cells = row.SelectNodes(".//td|.//th");

                if (cells is null || cells.Count < 2) continue;

                var label = cells[0].InnerText.Trim();
                var values = cells.Skip(1).Select(cell => cell.InnerText.Trim()).ToArray();

                Console.WriteLine($"Checking label: {label} with values: {string.Join(", ", values)}");

                foreach (var element in elementsOfInterest)
                {
                    if (label.Contains(element.Key, StringComparison.OrdinalIgnoreCase))
                    {
                        for (int i = 0; i < values.Length; i++)
                        {
                            string year = (2014 - i).ToString(); // Adjust this depending on your logic for extracting years
                            Console.WriteLine($"Matched element: {element.Key} -> {element.Value} with value: {values[i]} for year {year}");

                            SaveToDatabase(
                                $"{element.Value}_{year}",  // Use the database column name concatenated with the year
                                values[i],                  // The value extracted from the HTML
                                null,                       // context (not available in HTML, pass null or handle accordingly)
                                elementsOfInterest.Values.ToArray(),  // elementsOfInterest (as an array of values)
                                null,                       // elements (since we don't have dynamic elements from HTML, pass null or handle accordingly)
                                isAnnualReport              // isAnnualReport
                            );
                        }
                    }
                }
            }
        }
        else
        {
            Console.WriteLine("No table found in the HTML document.");
        }
    }

    static HashSet<string> GetElementsOfInterest()
    {
        return new HashSet<string>
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
    }

    private static bool IsRelevantPeriod(XElement context, bool isAnnualReport)
    {
        string startDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value;
        string endDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value;

        if (DateTime.TryParse(startDateStr, out DateTime startDate) && DateTime.TryParse(endDateStr, out DateTime endDate))
        {
            return isAnnualReport
                ? (endDate - startDate).Days is >= 360 and <= 370
                : (endDate - startDate).Days is >= 89 and <= 92;
        }

        return false;
    }

    private static int GetQuarter(DateTime date, bool isAnnualReport)
    {
        return isAnnualReport ? 0 : (date.Month + 2) / 3;
    }

    static void SaveToDatabase(string elementName, string value, XElement? context, string[] elementsOfInterest, List<FinancialElement> elements, bool isAnnualReport)
    {
        try
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();
            using var transaction = connection.BeginTransaction();
            using var command = new SqlCommand { Connection = connection, Transaction = transaction };
            try
            {
                command.CommandText = @"
                IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
                BEGIN
                    INSERT INTO CompaniesList (CompanyName, CompanySymbol, CompanyStockExchange, Industry, Sector)
                    VALUES (@CompanyName, @CompanySymbol, @CompanyStockExchange, @Industry, @Sector);
                END";
                command.Parameters.AddWithValue("@CompanyName", "Apple Inc.");
                command.Parameters.AddWithValue("@CompanySymbol", "AAPL");
                command.Parameters.AddWithValue("@CompanyStockExchange", "NASDAQ");
                command.Parameters.AddWithValue("@Industry", "Technology");
                command.Parameters.AddWithValue("@Sector", "Consumer Electronics");
                command.ExecuteNonQuery();

                Console.WriteLine("Company entry inserted/verified.");

                command.CommandText = "SELECT CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
                int companyId = (int)command.ExecuteScalar();
                Console.WriteLine($"Retrieved CompanyID: {companyId}");

                DateTime? startDate = null;
                DateTime? endDate = null;
                DateTime? instantDate = null;

                if (context != null)
                {
                    string startDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value;
                    string endDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value;
                    string instantDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "instant")?.Value;

                    startDate = DateTime.TryParse(startDateStr, out DateTime startDateValue) && IsValidSqlDateTime(startDateValue) ? startDateValue : (DateTime?)null;
                    endDate = DateTime.TryParse(endDateStr, out DateTime endDateValue) && IsValidSqlDateTime(endDateValue) ? endDateValue : (DateTime?)null;
                    instantDate = DateTime.TryParse(instantDateStr, out DateTime instantDateValue) && IsValidSqlDateTime(instantDateValue) ? instantDateValue : (DateTime?)null;
                }

                int year = startDate?.Year ?? endDate?.Year ?? instantDate?.Year ?? DateTime.Now.Year;
                int quarter = GetQuarter(startDate ?? endDate ?? instantDate ?? DateTime.Now, isAnnualReport);

                command.Parameters.Clear();
                command.CommandText = @"
                IF NOT EXISTS (SELECT 1 FROM Periods WHERE Year = @Year AND Quarter = @Quarter)
                BEGIN
                    INSERT INTO Periods (Year, Quarter) VALUES (@Year, @Quarter);
                    SELECT @PeriodID = SCOPE_IDENTITY();
                END
                ELSE
                BEGIN
                    SELECT @PeriodID = PeriodID FROM Periods WHERE Year = @Year AND Quarter = @Quarter;
                END";
                command.Parameters.AddWithValue("@Year", year);
                command.Parameters.AddWithValue("@Quarter", quarter);
                SqlParameter periodIdParam = new SqlParameter("@PeriodID", SqlDbType.Int) { Direction = ParameterDirection.Output };
                command.Parameters.Add(periodIdParam);
                command.ExecuteNonQuery();
                int periodId = (int)command.Parameters["@PeriodID"].Value;

                Console.WriteLine($"Period ID: {periodId} for Year: {year}, Quarter: {quarter}");

                command.Parameters.Clear();
                command.CommandText = @"
                SELECT FinancialDataID FROM FinancialData
                WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID AND (StatementType IS NULL OR StatementType = @StatementType)";
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@PeriodID", periodId);
                command.Parameters.AddWithValue("@StatementType", DBNull.Value);

                int? financialDataId = (int?)command.ExecuteScalar();

                if (financialDataId.HasValue)
                {
                    UpdateFinancialData(command, financialDataId.Value, elements, elementsOfInterest);
                }
                else
                {
                    InsertFinancialData(command, companyId, startDate, endDate, instantDate, elements, elementsOfInterest, periodId, year, quarter);
                }

                transaction.Commit();
                Console.WriteLine("Transaction committed successfully.");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine("Transaction error: " + ex.Message);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Database error: " + ex.Message);
        }
    }

    static void UpdateFinancialData(SqlCommand command, int financialDataId, List<FinancialElement> elements, string[] elementsOfInterest)
    {
        var setClauses = new StringBuilder();
        var addedParameters = new HashSet<string>();

        var filteredElements = elements
            .Where(e => elementsOfInterest.Contains(e.Name))
            .GroupBy(e => e.Name)
            .ToDictionary(g => g.Key, g => g.First());

        foreach (var element in elementsOfInterest)
        {
            if (filteredElements.TryGetValue(element, out var currentElement) && !string.IsNullOrEmpty(currentElement.Value) && addedParameters.Add(element))
            {
                setClauses.Append($"{element} = @{element}, ");
                command.Parameters.AddWithValue($"@{element}", currentElement.Value);
                Console.WriteLine($"Updating {element} with value: {currentElement.Value}");
            }
        }

        if (setClauses.Length > 0)
        {
            setClauses.Length -= 2;
            command.CommandText = $@"UPDATE FinancialData SET {setClauses} WHERE FinancialDataID = @FinancialDataID";
            command.Parameters.AddWithValue("@FinancialDataID", financialDataId);
            command.ExecuteNonQuery();
            Console.WriteLine("Financial data updated.");
        }
    }

    static void InsertFinancialData(SqlCommand command, int companyId, DateTime? startDate, DateTime? endDate, DateTime? instantDate, List<FinancialElement> elements, string[] elementsOfInterest, int periodId, int year, int quarter)
    {
        command.Parameters.Clear();
        var columnNames = new StringBuilder("CompanyID, PeriodID, Year, Quarter");
        var parameterNames = new StringBuilder("@CompanyID, @PeriodID, @Year, @Quarter");

        command.Parameters.AddWithValue("@CompanyID", companyId);
        command.Parameters.AddWithValue("@PeriodID", periodId);
        command.Parameters.AddWithValue("@Year", year);
        command.Parameters.AddWithValue("@Quarter", quarter);

        if (startDate.HasValue && endDate.HasValue && IsValidSqlDateTime(startDate.Value) && IsValidSqlDateTime(endDate.Value))
        {
            columnNames.Append(", StartDate, EndDate");
            parameterNames.Append(", @StartDate, @EndDate");
            command.Parameters.AddWithValue("@StartDate", startDate.Value.Date);
            command.Parameters.AddWithValue("@EndDate", endDate.Value.Date);
        }
        else if (instantDate.HasValue && IsValidSqlDateTime(instantDate.Value))
        {
            columnNames.Append(", InstantDate");
            parameterNames.Append(", @InstantDate");
            command.Parameters.AddWithValue("@InstantDate", instantDate.Value.Date);
        }
        else
        {
            Console.WriteLine("Invalid dates for insertion.");
            return;
        }

        var elementDictionary = elements
            .GroupBy(e => e.Name)
            .Select(g => g.First())
            .ToDictionary(e => e.Name, e => e.Value);

        foreach (var element in elementsOfInterest)
        {
            if (elementDictionary.ContainsKey(element) && elementDictionary[element]?.Equals(DBNull.Value) == false)
            {
                columnNames.Append($", {element}");
                parameterNames.Append($", @{element}");
                command.Parameters.AddWithValue($"@{element}", elementDictionary[element]);
                Console.WriteLine($"Inserting {element} with value: {elementDictionary[element]}");
            }
        }

        command.CommandText = $@"INSERT INTO FinancialData ({columnNames}) VALUES ({parameterNames})";
        command.ExecuteNonQuery();
        Console.WriteLine("Financial data inserted.");
    }

    private static bool IsValidSqlDateTime(DateTime dateTime)
    {
        return dateTime >= (DateTime)SqlDateTime.MinValue && dateTime <= (DateTime)SqlDateTime.MaxValue;
    }
}

public class FinancialElement
{
    public string Name { get; set; }
    public string? Value { get; set; }
    public string? ContextRef { get; set; } // If needed
}

