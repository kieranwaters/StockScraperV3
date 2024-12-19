using HtmlAgilityPack;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium;
using System.Data.SqlClient;
using System.Xml.Linq;
using Newtonsoft.Json;
using static Nasdaq100FinancialScraper.Program;
using Data;
using System.Data;
using System.Net.Http.Headers;
using System.Net;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Nasdaq100FinancialScraper;
using Newtonsoft.Json.Linq;

namespace StockScraperV3
{
    public class ChromeDriverPool : IDisposable
    {
        private readonly ConcurrentQueue<ChromeDriver> drivers = new ConcurrentQueue<ChromeDriver>();
        private readonly SemaphoreSlim semaphore;
        private readonly int maxDrivers;
        public ChromeDriverPool(int maxDrivers)
        {
            this.maxDrivers = maxDrivers;
            semaphore = new SemaphoreSlim(maxDrivers, maxDrivers);
            for (int i = 0; i < maxDrivers; i++)
            {
                var driver = StartNewSession();
                drivers.Enqueue(driver);
            }
        }
        public async Task<ChromeDriver> GetDriverAsync()
        {
            await semaphore.WaitAsync();
            if (drivers.TryDequeue(out var driver))
            { // Ensure the driver is still valid
                if (driver == null || driver.SessionId == null)
                {
                    driver = StartNewSession();
                }
                return driver;
            }
            else
            {
                semaphore.Release();
                throw new InvalidOperationException("No driver available.");
            }
        }
        public void ReturnDriver(ChromeDriver driver)
        {
            if (driver != null)
            {
                if (driver.SessionId != null)
                {
                    drivers.Enqueue(driver);
                }
                else
                {
                    driver.Quit();
                    var newDriver = StartNewSession();
                    drivers.Enqueue(newDriver);
                }
            }
            semaphore.Release();
        }
        public void Dispose()
        {
            while (drivers.TryDequeue(out var driver))
            {
                driver.Quit();
            }
            semaphore.Dispose();
        }
        private ChromeDriver StartNewSession()
        {
            ChromeOptions options = new ChromeOptions();
            options.AddArgument("--disable-gpu");            
            options.AddArgument("--window-size=1920x1080");
            options.AddArgument("--no-sandbox");
            options.AddArgument("--disable-dev-shm-usage");
            options.AddArgument("--ignore-certificate-errors"); // Disable SSL verification
            options.BinaryLocation = @"C:\Users\kiera\Downloads\chrome-win64\chrome-win64\chrome.exe";
            string chromeDriverPath = @"C:\Users\kiera\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe";
            return new ChromeDriver(chromeDriverPath, options);
        }
    }
    public class URL
    {
        private static readonly string connectionString = "Server=DESKTOP-SI08RN8\\SQLEXPRESS;Database=StockDataScraperDatabase;Integrated Security=True;";
        public static class HttpClientProvider
        {
            private static readonly Lazy<HttpClient> _httpClient = new Lazy<HttpClient>(() => CreateHttpClient());
            public static HttpClient Client => _httpClient.Value;
            public static HttpClient CreateHttpClient()
            {
                var handler = new HttpClientHandler
                {
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                    UseCookies = true,
                    CookieContainer = new CookieContainer()
                };
                var client = new HttpClient(handler);
                client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/html"));
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/xhtml+xml"));
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/xml", 0.9));
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*", 0.8));
                client.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate, br");
                client.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.9");
                client.DefaultRequestHeaders.Add("Connection", "keep-alive");
                return client;
            }
        }
        public static string NormaliseElementName(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return input;            
            var sanitized = Regex.Replace(input, @"[^a-zA-Z0-9\s]", "");// Remove any non-alphanumeric characters            
            var normalised = sanitized.ToUpper().Replace(" ", "_");// Convert to uppercase and replace spaces with underscores
            return normalised;
        }
        public void ExtractAndOutputUniqueXbrlElements()
        {
            // HashSet to store RawElementNames from XBRLDataTypes for quick lookup
            HashSet<string> rawElementNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // HashSet to keep track of already outputted element names to prevent duplicates
            HashSet<string> outputtedElements = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    // Step 1: Retrieve all RawElementNames from XBRLDataTypes
                    string rawElementNamesQuery = "SELECT RawElementName FROM XBRLDataTypes";
                    using (SqlCommand cmd = new SqlCommand(rawElementNamesQuery, connection))
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (!reader.IsDBNull(0))
                            {
                                string rawElementName = reader.GetString(0).Trim();
                                if (!string.IsNullOrEmpty(rawElementName))
                                {
                                    rawElementNames.Add(rawElementName);
                                }
                            }
                        }
                    }

                    // Step 2: Retrieve FinancialDataJSON from FinancialData
                    string financialDataQuery = "SELECT FinancialDataJSON FROM FinancialData";
                    using (SqlCommand cmd = new SqlCommand(financialDataQuery, connection))
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (reader.IsDBNull(0))
                                continue;

                            string json = reader.GetString(0);
                            if (string.IsNullOrWhiteSpace(json))
                                continue;

                            JObject financialData;
                            try
                            {
                                financialData = JObject.Parse(json);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error parsing JSON: {ex.Message}");
                                continue; // Skip this row and continue with the next
                            }

                            foreach (var property in financialData.Properties())
                            {
                                string key = property.Name;

                                // Skip keys containing "HTML"
                                if (key.IndexOf("HTML", StringComparison.OrdinalIgnoreCase) >= 0)
                                    continue;

                                // Check if the key is not in RawElementNames and hasn't been outputted yet
                                if (!rawElementNames.Contains(key) && !outputtedElements.Contains(key))
                                {
                                    Console.WriteLine(key);
                                    outputtedElements.Add(key);
                                }
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                Console.WriteLine($"Database error occurred: {sqlEx.Message}");
                // Handle or log exception as needed
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                // Handle or log exception as needed
            }
        }
        //public static async Task ProcessFilings(ChromeDriver driver, List<(string url, string description)> filings, string companyName, string companySymbol, int companyId, int groupIndex, DataNonStatic dataNonStatic) // Accept shared DataNonStatic instance
        //{
        //    int parsedReportsCount = 0;
        //    int totalReportsToParse = filings.Count(filing => filing.description.Contains("10-K") || filing.description.Contains("10-Q"));
        //    foreach (var filing in filings) // Process each filing
        //    {
        //        string url = filing.url;
        //        string description = filing.description;
        //        bool isAnnualReport = description.Contains("10-K");
        //        bool isQuarterlyReport = description.Contains("10-Q");
        //        bool isHtmlParsed = false;
        //        bool isXbrlParsed = false;
        //        int retries = 3;
        //        for (int attempt = 0; attempt < retries; attempt++)
        //        {
        //            try
        //            {    // XBRL Parsing
        //                var (xbrlUrl, isIxbrl) = await XBRL.XBRL.GetXbrlUrl(url);
        //                if (!string.IsNullOrEmpty(xbrlUrl))
        //                {
        //                    await XBRL.XBRL.DownloadAndParseXbrlData(xbrlUrl, isAnnualReport, companyName, companySymbol, 
        //                        async (content, isAnnual, name, symbol) =>
        //                        {
        //                            if (isIxbrl)
        //                            {
        //                                await XBRL.XBRL.ParseInlineXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
        //                            }
        //                            else
        //                            {
        //                                await XBRL.XBRL.ParseTraditionalXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
        //                            }
        //                        },
        //                        async (content, isAnnual, name, symbol) =>
        //                        {
        //                            if (!isIxbrl)
        //                            {
        //                                await XBRL.XBRL.ParseInlineXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
        //                            }
        //                        });
        //                    isXbrlParsed = true;
        //                }
        //                else
        //                {
        //                    Console.WriteLine($"[WARNING] No XBRL URL obtained for {companyName} ({companySymbol}) filing: {description}");
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                if (attempt == retries - 1)
        //                {
        //                    Console.WriteLine($"[ERROR] XBRL Parsing failed for {companyName} ({companySymbol}) on attempt {attempt + 1}: {ex.Message}");
        //                    throw;
        //                }
        //                Console.WriteLine($"[WARNING] XBRL Parsing attempt {attempt + 1} failed for {companyName} ({companySymbol}): {ex.Message}");
        //                await Task.Delay(1000); // Optional: Add delay before retrying
        //                continue;
        //            }
        //            try
        //            {
        //                // HTML Parsing
        //                string? interactiveDataUrl = await URL.GetInteractiveDataUrl(url);
        //                if (!string.IsNullOrEmpty(interactiveDataUrl))
        //                {
        //                    await HTML.HTML.ProcessInteractiveData(driver, interactiveDataUrl, companyName, companySymbol, isAnnualReport, url, companyId, dataNonStatic);
        //                    isHtmlParsed = true;
        //                }
        //                else
        //                {
        //                    Console.WriteLine($"[WARNING] No Interactive Data URL obtained for {companyName} ({companySymbol}) filing: {description}");
        //                }
        //            }
        //            catch (WebDriverException ex)
        //            {
        //                if (ex.Message.Contains("disconnected"))
        //                {
        //                    Console.WriteLine($"[ERROR] ChromeDriver disconnected for {companyName}. Attempt {attempt + 1} of {retries}.");
        //                    throw; // Let the calling method handle driver replacement via the pool
        //                }
        //                else
        //                {
        //                    Console.WriteLine($"[ERROR] WebDriverException for {companyName}: {ex.Message}");
        //                    if (attempt == retries - 1)
        //                    {
        //                        throw;
        //                    }
        //                    Console.WriteLine($"[WARNING] HTML Parsing attempt {attempt + 1} failed for {companyName}: {ex.Message}");
        //                    await Task.Delay(1000); // Optional: Add delay before retrying
        //                    continue;
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                if (attempt == retries - 1)
        //                {
        //                    Console.WriteLine($"[ERROR] HTML Parsing failed for {companyName} ({companySymbol}) on attempt {attempt + 1}: {ex.Message}");
        //                    throw;
        //                }
        //                Console.WriteLine($"[WARNING] HTML Parsing attempt {attempt + 1} failed for {companyName} ({companySymbol}): {ex.Message}");
        //                await Task.Delay(1000); // Optional: Add delay before retrying
        //                continue;
        //            }
        //            break; // Exit retry loop if successful
        //        }
        //    }

        //}
        public static async Task ProcessFilings(
    ChromeDriver driver,
    List<(string url, string description)> filings,
    string companyName,
    string companySymbol,
    int companyId,
    int groupIndex,
    DataNonStatic dataNonStatic)
        {
            int parsedReportsCount = 0;

            // Update totalReportsToParse to include all relevant report types
            int totalReportsToParse = filings.Count(filing =>
                filing.description.Contains("10-K", StringComparison.OrdinalIgnoreCase) ||
                filing.description.Contains("10-Q", StringComparison.OrdinalIgnoreCase) ||
                filing.description.Contains("20-F", StringComparison.OrdinalIgnoreCase) ||
                filing.description.Contains("6-K", StringComparison.OrdinalIgnoreCase) ||
                filing.description.Contains("40-F", StringComparison.OrdinalIgnoreCase) ||
                filing.description.Contains("8-K", StringComparison.OrdinalIgnoreCase));

            foreach (var filing in filings) // Process each filing
            {
                string url = filing.url;
                string description = filing.description;

                // Extract the report type from the description
                string reportType = ExtractReportType(description);
                ReportCategory category = GetReportCategory(reportType);

                bool isAnnualReport = category == ReportCategory.Annual;
                bool isQuarterlyReport = category == ReportCategory.Quarterly;
                bool isCurrentReport = category == ReportCategory.Current;

                bool isHtmlParsed = false;
                bool isXbrlParsed = false;
                int retries = 3;

                for (int attempt = 0; attempt < retries; attempt++)
                {
                    try
                    {
                        // XBRL Parsing
                        var (xbrlUrl, isIxbrl) = await XBRL.XBRL.GetXbrlUrl(url);
                        if (!string.IsNullOrEmpty(xbrlUrl))
                        {
                            await XBRL.XBRL.DownloadAndParseXbrlData(
                                xbrlUrl,
                                isAnnualReport: isAnnualReport,
                                companyName: companyName,
                                companySymbol: companySymbol,
                                // XBRL Callback for parsing
                                async (content, isAnnual, name, symbol) =>
                                {
                                    if (isIxbrl)
                                    {
                                        await XBRL.XBRL.ParseInlineXbrlContent(
                                            content,
                                            isAnnual,
                                            name,
                                            symbol,
                                            dataNonStatic,
                                            companyId);
                                    }
                                    else
                                    {
                                        await XBRL.XBRL.ParseTraditionalXbrlContent(
                                            content,
                                            isAnnual,
                                            name,
                                            symbol,
                                            dataNonStatic,
                                            companyId);
                                    }
                                },
                                // Alternative XBRL Callback
                                async (content, isAnnual, name, symbol) =>
                                {
                                    if (!isIxbrl)
                                    {
                                        await XBRL.XBRL.ParseInlineXbrlContent(
                                            content,
                                            isAnnual,
                                            name,
                                            symbol,
                                            dataNonStatic,
                                            companyId);
                                    }
                                });

                            isXbrlParsed = true;
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] No XBRL URL obtained for {companyName} ({companySymbol}) filing: {description}");
                        }
                    }
                    catch (Exception ex)
                    {
                        if (attempt == retries - 1)
                        {
                            Console.WriteLine($"[ERROR] XBRL Parsing failed for {companyName} ({companySymbol}) on attempt {attempt + 1}: {ex.Message}");
                            throw;
                        }

                        Console.WriteLine($"[WARNING] XBRL Parsing attempt {attempt + 1} failed for {companyName} ({companySymbol}): {ex.Message}");
                        await Task.Delay(1000); // Optional: Add delay before retrying
                        continue;
                    }

                    try
                    {
                        // HTML Parsing
                        string? interactiveDataUrl = await URL.GetInteractiveDataUrl(url);
                        if (!string.IsNullOrEmpty(interactiveDataUrl))
                        {
                            await HTML.HTML.ProcessInteractiveData(
                                driver,
                                interactiveDataUrl,
                                companyName,
                                companySymbol,
                                isAnnualReport,
                                url,
                                companyId,
                                dataNonStatic);

                            isHtmlParsed = true;
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] No Interactive Data URL obtained for {companyName} ({companySymbol}) filing: {description}");
                        }
                    }
                    catch (WebDriverException ex)
                    {
                        if (ex.Message.Contains("disconnected", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[ERROR] ChromeDriver disconnected for {companyName}. Attempt {attempt + 1} of {retries}.");
                            throw; // Let the calling method handle driver replacement via the pool
                        }
                        else
                        {
                            Console.WriteLine($"[ERROR] WebDriverException for {companyName}: {ex.Message}");
                            if (attempt == retries - 1)
                            {
                                throw;
                            }
                            Console.WriteLine($"[WARNING] HTML Parsing attempt {attempt + 1} failed for {companyName}: {ex.Message}");
                            await Task.Delay(1000); // Optional: Add delay before retrying
                            continue;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (attempt == retries - 1)
                        {
                            Console.WriteLine($"[ERROR] HTML Parsing failed for {companyName} ({companySymbol}) on attempt {attempt + 1}: {ex.Message}");
                            throw;
                        }
                        Console.WriteLine($"[WARNING] HTML Parsing attempt {attempt + 1} failed for {companyName} ({companySymbol}): {ex.Message}");
                        await Task.Delay(1000); // Optional: Add delay before retrying
                        continue;
                    }

                    // If either XBRL or HTML parsing succeeded, increment the counter
                    if (isXbrlParsed || isHtmlParsed)
                    {
                        parsedReportsCount++;
                    }

                    break; // Exit retry loop if successful
                }

                // Optional: Log progress
                Console.WriteLine($"[INFO] Parsed {parsedReportsCount}/{totalReportsToParse} reports for {companyName} ({companySymbol}).");
            }
        }
            private static string ExtractReportType(string description)
            {
                if (string.IsNullOrWhiteSpace(description))
                    return string.Empty;

                // Example: "10-K Annual Report" -> "10-K"
                var parts = description.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                return parts.Length > 0 ? parts[0] : string.Empty;
            }
            private static ReportCategory GetReportCategory(string reportType)
            {
                switch (reportType.ToUpperInvariant())
                {
                    case "10-K":
                    case "20-F":
                    case "40-F":
                        return ReportCategory.Annual;
                    case "10-Q":
                    case "6-K":
                        return ReportCategory.Quarterly;
                    case "8-K":
                        return ReportCategory.Current;
                    default:
                        return ReportCategory.Unknown;
                }
            } 
    private enum ReportCategory
        {
            Annual,
            Quarterly,
            Current,
            Unknown
        }
    


    private static async Task<(int companyId, string companyName, string symbol, int cik)> GetCompanyBySymbol(string companySymbol)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                string query = "SELECT CompanyID, CompanyName, CompanySymbol, CIK FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            int companyId = reader.GetInt32(reader.GetOrdinal("CompanyID"));
                            string companyName = reader.GetString(reader.GetOrdinal("CompanyName"));
                            string symbol = reader.GetString(reader.GetOrdinal("CompanySymbol"));
                            int cik = reader.GetInt32(reader.GetOrdinal("CIK"));
                            return (companyId, companyName, symbol, cik);
                        }
                        else
                        {
                            return default;
                        }
                    }
                }
            }
        }
        public static async Task<string> ScrapeReportsForCompanyAsync(string companySymbol)
        {
            try
            {
                Console.WriteLine($"[INFO] Starting to scrape reports for {companySymbol}.");                
                var company = await GetCompanyBySymbol(companySymbol);// 1. Retrieve company details from the database
                if (company == default)
                {
                    return $"Error: Company with symbol {companySymbol} not found.";
                }
                using (var driverPool = new ChromeDriverPool(5))
                {
                    var semaphore = new SemaphoreSlim(5, 5);
                    var filingTypes = new[] { "10-K", "10-Q", "20-F", "6-K", "40-F", "8-K" };

                    var filingTasks = filingTypes.Select(async filingType =>
                    {
                        return await GetFilingUrlsForLast10Years(company.cik.ToString(), filingType);
                    });
                    var filingsResults = await Task.WhenAll(filingTasks);
                    var filings = filingsResults
                        .SelectMany(f => f)
                        .DistinctBy(f => f.url)
                        .Select(f => (f.url, f.description))
                        .ToList();
                    if (!filings.Any())
                    {
                        Console.WriteLine($"[INFO] No filings found for {company.companyName} ({company.symbol}).");
                        return $"No filings found for {company.companyName} ({company.symbol}).";
                    }
                    Console.WriteLine($"[INFO] Found {filings.Count} filings for {company.companyName} ({company.symbol}).");
                    var dataNonStatic = new DataNonStatic();
                    var processingTasks = new List<Task>();
                    foreach (var filing in filings)
                    {
                        await semaphore.WaitAsync();
                        var task = Task.Run(async () =>
                        {
                            ChromeDriver driver = null;
                            try
                            {
                                driver = await driverPool.GetDriverAsync();
                                await ProcessFilingAsync(filing, company, driver, dataNonStatic);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ERROR] Exception processing filing {filing.description} for {company.companyName}: {ex.Message}");
                            }
                            finally
                            {
                                if (driver != null)
                                {
                                    driverPool.ReturnDriver(driver);
                                }
                                semaphore.Release();
                            }
                        });
                        processingTasks.Add(task);
                    } // Await all processing tasks
                    await Task.WhenAll(processingTasks);
                    await PostProcessCompanyDataAsync(company, dataNonStatic);
                    Console.WriteLine($"[INFO] Finished scraping reports for {company.companyName} ({company.symbol}).");
                    return $"Successfully scraped {filings.Count} filings for {company.companyName} ({company.symbol}).";
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to scrape reports for {companySymbol}: {ex.Message}");
                return $"Error: {ex.Message}";
            }
        }
        private static async Task PostProcessCompanyDataAsync((int companyId, string companyName, string symbol, int cik) company, DataNonStatic dataNonStatic)
        {    // Adjust Q2 and Q3 Cashflows from Cumulative to Actual Values
            Console.WriteLine($"[INFO] Adjusting Q2 and Q3 Cashflows for {company.companyName} ({company.symbol})");
            var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(company.companyId);
            var currentFiscalYear = GetCurrentFiscalYear(companyData);
            var entriesForYear = companyData.FinancialEntries.Values
                .Where(e => e.Year == currentFiscalYear)
                .ToList();
            var Q1entry = entriesForYear.FirstOrDefault(e => e.Quarter == 1);
            var Q2entry = entriesForYear.FirstOrDefault(e => e.Quarter == 2);
            var Q3entry = entriesForYear.FirstOrDefault(e => e.Quarter == 3);
            if (Q1entry != null && Q2entry != null && Q3entry != null)
            {    // Gather all keys that appear in these quarters
                var allKeys = new HashSet<string>(
                    Q1entry.FinancialValues.Keys
                    .Concat(Q2entry.FinancialValues.Keys)
                    .Concat(Q3entry.FinancialValues.Keys),
                    StringComparer.OrdinalIgnoreCase);
                foreach (var key in allKeys)
                {     // We only adjust HTML-based Cashflow items
                    if (key.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
                    {
                        string statementType = Data.Data.GetStatementType(key).ToLowerInvariant();
                        bool isCashFlow = statementType.Contains("cashflow");
                        if (isCashFlow)
                        {                           
                            string baseKey = RemoveQuarterFromElementName(key);
                            string q1Key = AdjustElementNameForQuarter(baseKey, 1);
                            string q2Key = AdjustElementNameForQuarter(baseKey, 2);
                            string q3Key = AdjustElementNameForQuarter(baseKey, 3);
                            // Extract numeric values using the updated ExtractNumericValue method
                            decimal Q1_cum = ExtractNumericValue(Q1entry.FinancialValues.TryGetValue(q1Key, out var Q1_obj) ? Q1_obj : null, q1Key);
                            decimal Q2_cum = ExtractNumericValue(Q2entry.FinancialValues.TryGetValue(q2Key, out var Q2_obj) ? Q2_obj : null, q2Key);
                            decimal Q3_cum = ExtractNumericValue(Q3entry.FinancialValues.TryGetValue(q3Key, out var Q3_obj) ? Q3_obj : null, q3Key);                    
                            if (Q2_cum == 0 || Q3_cum == 0)// Ensure Q2_cum and Q3_cum are valid
                            {
                                Console.WriteLine($"[WARNING] Invalid cumulative value for {baseKey}. Q2_cum: {Q2_cum}, Q3_cum: {Q3_cum}. Skipping adjustment.");
                                continue;
                            }
                            decimal Q2_actual = Q2_cum - Q1_cum;
                            decimal Q3_actual = Q3_cum - Q2_cum;
                            if (Q2_actual < 0 || Q3_actual < 0)
                            {
                                Console.WriteLine($"[WARNING] Negative actual values for {baseKey}. Q2_actual: {Q2_actual}, Q3_actual: {Q3_actual}. Skipping.");
                                continue;
                            }                 // Update the entries with actual values
                            Q2entry.FinancialValues[q2Key] = Q2_actual;
                            Q3entry.FinancialValues[q3Key] = Q3_actual;
                        }
                    }
                }
            }
            var adjustedEntries = companyData.GetCompletedEntries();
            if (adjustedEntries.Any())
            {
                await dataNonStatic.SaveAllEntriesToDatabaseAsync(company.companyId);
            }
            else
            {
                Console.WriteLine($"[INFO] No completed entries to save for {company.companyName} ({company.symbol}).");
            }
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await Data.Data.CalculateAndSaveQ4InDatabaseAsync(connection, transaction, company.companyId, dataNonStatic);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Transaction failed while calculating/saving Q4 for {company.companyName} ({company.symbol}): {ex.Message}");
                        try
                        {
                            transaction.Rollback();
                            Console.WriteLine($"Transaction rolled back for {company.companyName} ({company.symbol}).");
                        }
                        catch (Exception rollbackEx)
                        {
                            Console.WriteLine($"[ERROR] Transaction rollback failed for {company.companyName} ({company.symbol}): {rollbackEx.Message}");
                        }
                    }
                }
            }
        }
        public static async Task<List<(string url, string description, DateTime filingDate)>> GetFilingUrlsForLast10Years(string companyCIK, string filingType)
        {
            string url = $"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={companyCIK}&type={filingType}&count=100&output=atom";
            List<(string url, string description, DateTime filingDate)> filings = new List<(string url, string description, DateTime filingDate)>();
            int retries = 3;
            int delayMilliseconds = 5000;
            for (int i = 0; i < retries; i++)
            {
                try
                {     // Delay between requests
                    await Task.Delay(delayMilliseconds);
                    string response = await HttpClientProvider.Client.GetStringAsync(url);
                    XNamespace atom = "http://www.w3.org/2005/Atom";// Parse the XML response and extract the relevant data
                    filings = XDocument.Parse(response)
                        .Descendants(atom + "entry")
                        .Where(entry => DateTime.Parse(entry.Element(atom + "updated")?.Value ?? DateTime.MinValue.ToString()).Year >= DateTime.Now.Year - 10)
                        .Select(entry => (
                            url: entry.Element(atom + "link")?.Attribute("href")?.Value!,
                            description: entry.Element(atom + "title")?.Value ?? string.Empty,
                            filingDate: DateTime.Parse(entry.Element(atom + "updated")?.Value ?? DateTime.MinValue.ToString())
                        ))
                        .ToList();
                    break; // Exit loop after successful request
                }
                catch (HttpRequestException ex) when ((int)ex.StatusCode == 429)
                {        // Handle 429 Too Many Requests response by waiting before retrying
                    await Task.Delay(delayMilliseconds * (i + 1)); // Exponential back-off
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Failed to retrieve filings: {ex.Message}");
                    break; // Exit the loop if another error occurs
                }
            }
            return filings;
        }
        public static async Task ProcessFilingAsync((string url, string description) filing, (int companyId, string companyName, string symbol, int cik) company, ChromeDriver driver, DataNonStatic dataNonStatic)
        {
            try
            {   // Process the filing using the provided driver
                await ProcessFilings(driver, new List<(string url, string description)> { filing }, company.companyName, company.symbol, company.companyId, 0, dataNonStatic);
                Console.WriteLine($"[INFO] Successfully scraped filing {filing.description} for {company.companyName} ({company.symbol})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to scrape filing {filing.description} for {company.companyName}: {ex.Message}");
                throw; // Re-throw the exception to allow higher-level handling if necessary
            }
        }
        public static async Task RunScraperAsync()
        {
            var nasdaq100Companies = await GetNasdaq100CompaniesFromDatabase(); //Get Nasdaq100 companies from DB
            var driverPool = new ChromeDriverPool(10); //Initialize pool with 5 drivers
            var semaphore = new SemaphoreSlim(5); //Limit concurrency to match the driver pool size
            var dataNonStatic = new DataNonStatic(); //Initialize a shared DataNonStatic instance
            foreach (var company in nasdaq100Companies)
            {
                var localCompany = company; //Local reference for company
                Console.WriteLine($"Starting processing for {localCompany.companyName} ({localCompany.symbol})");
                var filingTasks = new[]
                {
            StockScraperV3.URL.GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-K"), //Fetch 10-K
            StockScraperV3.URL.GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-Q")  //Fetch 10-Q
        };
                var filingsResults = await Task.WhenAll(filingTasks); //Wait for both tasks
                var filings = filingsResults.SelectMany(f => f).DistinctBy(f => f.url).Select(f => (f.url, f.description)).ToList(); //Combine and remove duplicates
                if (!filings.Any())
                {
                    Console.WriteLine($"No filings found for {localCompany.companyName} ({localCompany.symbol})");
                    continue;
                }
                var chromeDriverTasks = new List<Task>();
                foreach (var filing in filings)
                {
                    await semaphore.WaitAsync(); //Wait for a slot
                    chromeDriverTasks.Add(Task.Run(async () =>
                    {
                        ChromeDriver driver = null;
                        try
                        {
                            driver = await driverPool.GetDriverAsync(); //Get a driver from the pool
                            await ProcessFilingAsync(filing, localCompany, driver, dataNonStatic); //Process the filing
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Exception during filing processing for {localCompany.companyName} ({localCompany.symbol}): {ex.Message}");
                        }
                        finally
                        {
                            if (driver != null)
                            {
                                driverPool.ReturnDriver(driver); //Return driver to pool
                            }
                            semaphore.Release(); //Release the semaphore
                        }
                    }));
                }
                await Task.WhenAll(chromeDriverTasks); //Wait for all filings to be processed
                var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(localCompany.companyId); //Load company data
                var currentFiscalYear = GetCurrentFiscalYear(companyData); //Get current fiscal year
                var entriesForYear = companyData.FinancialEntries.Values.Where(e => e.Year == currentFiscalYear).ToList(); //Entries of that year
                var Q1entry = entriesForYear.FirstOrDefault(e => e.Quarter == 1);
                var Q2entry = entriesForYear.FirstOrDefault(e => e.Quarter == 2);
                var Q3entry = entriesForYear.FirstOrDefault(e => e.Quarter == 3);
                if (Q1entry != null && Q2entry != null && Q3entry != null)
                {
                    var allKeys = new HashSet<string>(Q1entry.FinancialValues.Keys.Concat(Q2entry.FinancialValues.Keys).Concat(Q3entry.FinancialValues.Keys), StringComparer.OrdinalIgnoreCase);
                    foreach (var key in allKeys)
                    {
                        if (key.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
                        {
                            string statementType = Data.Data.GetStatementType(key).ToLowerInvariant();
                            bool isCashFlow = statementType.Contains("cashflow");
                            if (isCashFlow)
                            {
                                string baseKey = RemoveQuarterFromElementName(key);
                                string q1Key = AdjustElementNameForQuarter(baseKey, 1);
                                string q2Key = AdjustElementNameForQuarter(baseKey, 2);
                                string q3Key = AdjustElementNameForQuarter(baseKey, 3);
                                decimal Q1_cum = ExtractNumericValue(Q1entry.FinancialValues.TryGetValue(q1Key, out var Q1_obj) ? Q1_obj : null, q1Key);
                                decimal Q2_cum = ExtractNumericValue(Q2entry.FinancialValues.TryGetValue(q2Key, out var Q2_obj) ? Q2_obj : null, q2Key);
                                decimal Q3_cum = ExtractNumericValue(Q3entry.FinancialValues.TryGetValue(q3Key, out var Q3_obj) ? Q3_obj : null, q3Key);
                                if (Q2_cum == 0 || Q3_cum == 0)
                                {
                                    Console.WriteLine($"[WARNING] Invalid cumulative value for {baseKey}. Q2_cum: {Q2_cum}, Q3_cum: {Q3_cum}. Skipping adjustment.");
                                    continue;
                                }
                                decimal Q2_actual = Q2_cum - Q1_cum;
                                decimal Q3_actual = Q3_cum - Q2_cum;
                                if (Q2_actual < 0 || Q3_actual < 0)
                                {
                                    Console.WriteLine($"[WARNING] Negative actual values for {baseKey}. Q2_actual: {Q2_actual}, Q3_actual: {Q3_actual}. Skipping.");
                                    continue;
                                }
                                Q2entry.FinancialValues[q2Key] = Q2_actual;
                                Q3entry.FinancialValues[q3Key] = Q3_actual;
                            }
                        }
                    }
                }
                Data.Data.AdjustAnnualReportYears(companyData); //Call the adjust method here before saving
                var adjustedEntries = companyData.GetCompletedEntries(); //Get completed entries
                if (adjustedEntries.Any())
                {
                    await dataNonStatic.SaveAllEntriesToDatabaseAsync(localCompany.companyId); //Save to DB
                }
                else
                {
                    Console.WriteLine($"[INFO] No completed entries to save for {localCompany.companyName} ({localCompany.symbol}).");
                }
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    using (SqlTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            await Data.Data.CalculateAndSaveQ4InDatabaseAsync(connection, transaction, localCompany.companyId, dataNonStatic); //Q4 calculation
                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Transaction failed while calculating/saving Q4 for {localCompany.companyName} ({localCompany.symbol}): {ex.Message}");
                            try
                            {
                                transaction.Rollback();
                                Console.WriteLine($"Transaction rolled back for {localCompany.companyName} ({localCompany.symbol}).");
                            }
                            catch (Exception rollbackEx)
                            {
                                Console.WriteLine($"[ERROR] Transaction rollback failed for {localCompany.companyName} ({localCompany.symbol}): {rollbackEx.Message}");
                            }
                        }
                    }
                }
                Console.WriteLine($"Finished processing for {localCompany.companyName} ({localCompany.symbol})");
            }
            driverPool.Dispose(); //Dispose driver pool
            semaphore.Dispose();  //Dispose semaphore
        }
        // Define a list of all report types you want to handle
        private static readonly string[] ReportTypes = new[]
        {
    "10-K",
    "10-Q",
    "20-F",
    "6-K",
    "40-F",
    "8-K"
};


        public static async Task ContinueScrapingUnscrapedCompaniesAsync()
        {
            try
            {
                var unscrapedCompanies = await GetCompaniesWithHigherCompanyIDAsync(); //Get unscraped companies
                Console.WriteLine($"[INFO] Retrieved {unscrapedCompanies.Count} unscraped companies.");
                if (!unscrapedCompanies.Any())
                {
                    Console.WriteLine("[INFO] No unscraped companies found.");
                    return;
                }
                using (var driverPool = new ChromeDriverPool(10))
                {
                    var semaphore = new SemaphoreSlim(5, 5);
                    var dataNonStatic = new DataNonStatic();
                    var reportTypes = new[] { "10-K", "10-Q", "20-F", "6-K", "40-F", "8-K" };
                    foreach (var company in unscrapedCompanies)
                    {
                        Console.WriteLine($"[INFO] Processing company: {company.companyName} ({company.symbol})");
                        var filingTasks = reportTypes.Select(type =>
                    GetFilingUrlsForLast10Years(company.cik.ToString(), type)).ToArray();
                        var filingsResults = await Task.WhenAll(filingTasks);
                        var filings = filingsResults.SelectMany(f => f).DistinctBy(f => f.url).Select(f => (f.url, f.description)).ToList();
                        if (!filings.Any())
                        {
                            Console.WriteLine($"[INFO] No filings found for {company.companyName} ({company.symbol}).");
                            continue;
                        }
                        Console.WriteLine($"[INFO] Found {filings.Count} filings for {company.companyName} ({company.symbol}).");
                        var filingProcessingTasks = new List<Task>();
                        foreach (var filing in filings)
                        {
                            await semaphore.WaitAsync();
                            var task = Task.Run(async () =>
                            {
                                ChromeDriver driver = null;
                                try
                                {
                                    driver = await driverPool.GetDriverAsync();
                                    await ProcessFilingAsync(filing, company, driver, dataNonStatic);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[ERROR] Exception processing filing {filing.description} for {company.companyName}: {ex.Message}");
                                }
                                finally
                                {
                                    if (driver != null)
                                    {
                                        driverPool.ReturnDriver(driver);
                                    }
                                    semaphore.Release();
                                }
                            });
                            filingProcessingTasks.Add(task);
                        }
                        await Task.WhenAll(filingProcessingTasks);
                        var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(company.companyId);
                        var currentFiscalYear = GetCurrentFiscalYear(companyData);
                        var entriesForYear = companyData.FinancialEntries.Values.Where(e => e.Year == currentFiscalYear).ToList();
                        var Q1entry = entriesForYear.FirstOrDefault(e => e.Quarter == 1);
                        var Q2entry = entriesForYear.FirstOrDefault(e => e.Quarter == 2);
                        var Q3entry = entriesForYear.FirstOrDefault(e => e.Quarter == 3);
                        if (Q1entry != null && Q2entry != null && Q3entry != null)
                        {
                            var allKeys = new HashSet<string>(Q1entry.FinancialValues.Keys.Concat(Q2entry.FinancialValues.Keys).Concat(Q3entry.FinancialValues.Keys), StringComparer.OrdinalIgnoreCase);
                            foreach (var key in allKeys)
                            {
                                if (key.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
                                {
                                    string statementType = Data.Data.GetStatementType(key).ToLowerInvariant();
                                    bool isCashFlow = statementType.Contains("cashflow");
                                    if (isCashFlow)
                                    {
                                        string baseKey = RemoveQuarterFromElementName(key);
                                        string q1Key = AdjustElementNameForQuarter(baseKey, 1);
                                        string q2Key = AdjustElementNameForQuarter(baseKey, 2);
                                        string q3Key = AdjustElementNameForQuarter(baseKey, 3);
                                        decimal Q1_cum = ExtractNumericValue(Q1entry.FinancialValues.TryGetValue(q1Key, out var Q1_obj) ? Q1_obj : null, q1Key);
                                        decimal Q2_cum = ExtractNumericValue(Q2entry.FinancialValues.TryGetValue(q2Key, out var Q2_obj) ? Q2_obj : null, q2Key);
                                        decimal Q3_cum = ExtractNumericValue(Q3entry.FinancialValues.TryGetValue(q3Key, out var Q3_obj) ? Q3_obj : null, q3Key);
                                        if (Q2_cum == 0 || Q3_cum == 0)
                                        {
                                            Console.WriteLine($"[WARNING] Invalid cumulative value for {baseKey}. Q2_cum: {Q2_cum}, Q3_cum: {Q3_cum}. Skipping adjustment.");
                                            continue;
                                        }
                                        decimal Q2_actual = Q2_cum - Q1_cum;
                                        decimal Q3_actual = Q3_cum - Q2_cum;
                                        if (Q2_actual < 0 || Q3_actual < 0)
                                        {
                                            Console.WriteLine($"[WARNING] Negative actual values for {baseKey}. Q2_actual: {Q2_actual}, Q3_actual: {Q3_actual}. Skipping.");
                                            continue;
                                        }
                                        Q2entry.FinancialValues[q2Key] = Q2_actual;
                                        Q3entry.FinancialValues[q3Key] = Q3_actual;
                                    }
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] Missing Q1, Q2, or Q3 entries for {company.companyName} ({company.symbol}). Skipping cashflow adjustments.");
                        }
                        Data.Data.AdjustAnnualReportYears(companyData); //Call the adjust method here before saving
                        var adjustedEntries = companyData.GetCompletedEntries();
                        if (adjustedEntries.Any())
                        {
                            await dataNonStatic.SaveAllEntriesToDatabaseAsync(company.companyId);
                        }
                        else
                        {
                            Console.WriteLine($"[INFO] No completed entries to save for {company.companyName} ({company.symbol}).");
                        }
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            await connection.OpenAsync();
                            using (SqlTransaction transaction = connection.BeginTransaction())
                            {
                                try
                                {
                                    await Data.Data.CalculateAndSaveQ4InDatabaseAsync(connection, transaction, company.companyId, dataNonStatic);
                                    transaction.Commit();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[ERROR] Transaction failed while calculating/saving Q4 for {company.companyName} ({company.symbol}): {ex.Message}");
                                    try
                                    {
                                        transaction.Rollback();
                                        Console.WriteLine($"Transaction rolled back for {company.companyName} ({company.symbol}).");
                                    }
                                    catch (Exception rollbackEx)
                                    {
                                        Console.WriteLine($"[ERROR] Transaction rollback failed for {company.companyName} ({company.symbol}): {rollbackEx.Message}");
                                    }
                                }
                            }
                        }
                        Console.WriteLine($"Finished processing for {company.companyName} ({company.symbol})");
                    }
                    semaphore.Dispose();
                }
                Console.WriteLine("[INFO] RunScraperForUnscrapedCompaniesAsync completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in RunScraperForUnscrapedCompaniesAsync: {ex.Message}");
            }
        }

        public static async Task RunScraperForUnscrapedCompaniesAsync()
        {
            try
            {
                Console.WriteLine("[INFO] Starting RunScraperForUnscrapedCompaniesAsync.");
                var unscrapedCompanies = await GetUnscrapedCompaniesFromDatabase(); //Get unscraped companies
                Console.WriteLine($"[INFO] Retrieved {unscrapedCompanies.Count} unscraped companies.");
                if (!unscrapedCompanies.Any())
                {
                    Console.WriteLine("[INFO] No unscraped companies found.");
                    return;
                }
                using (var driverPool = new ChromeDriverPool(10))
                {
                    var semaphore = new SemaphoreSlim(5, 5);
                    var dataNonStatic = new DataNonStatic();
                    var reportTypes = new[] { "10-K", "10-Q", "20-F", "6-K", "40-F", "8-K" };
                    foreach (var company in unscrapedCompanies)
                    {
                        Console.WriteLine($"[INFO] Processing company: {company.companyName} ({company.symbol})");
                        var filingTasks = reportTypes.Select(type =>
                    GetFilingUrlsForLast10Years(company.cik.ToString(), type)).ToArray();
                        var filingsResults = await Task.WhenAll(filingTasks);
                        var filings = filingsResults.SelectMany(f => f).DistinctBy(f => f.url).Select(f => (f.url, f.description)).ToList();
                        if (!filings.Any())
                        {
                            Console.WriteLine($"[INFO] No filings found for {company.companyName} ({company.symbol}).");
                            continue;
                        }
                        Console.WriteLine($"[INFO] Found {filings.Count} filings for {company.companyName} ({company.symbol}).");
                        var filingProcessingTasks = new List<Task>();
                        foreach (var filing in filings)
                        {
                            await semaphore.WaitAsync();
                            var task = Task.Run(async () =>
                            {
                                ChromeDriver driver = null;
                                try
                                {
                                    driver = await driverPool.GetDriverAsync();
                                    await ProcessFilingAsync(filing, company, driver, dataNonStatic);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[ERROR] Exception processing filing {filing.description} for {company.companyName}: {ex.Message}");
                                }
                                finally
                                {
                                    if (driver != null)
                                    {
                                        driverPool.ReturnDriver(driver);
                                    }
                                    semaphore.Release();
                                }
                            });
                            filingProcessingTasks.Add(task);
                        }
                        await Task.WhenAll(filingProcessingTasks);
                        var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(company.companyId);
                        var currentFiscalYear = GetCurrentFiscalYear(companyData);
                        var entriesForYear = companyData.FinancialEntries.Values.Where(e => e.Year == currentFiscalYear).ToList();
                        var Q1entry = entriesForYear.FirstOrDefault(e => e.Quarter == 1);
                        var Q2entry = entriesForYear.FirstOrDefault(e => e.Quarter == 2);
                        var Q3entry = entriesForYear.FirstOrDefault(e => e.Quarter == 3);
                        if (Q1entry != null && Q2entry != null && Q3entry != null)
                        {
                            var allKeys = new HashSet<string>(Q1entry.FinancialValues.Keys.Concat(Q2entry.FinancialValues.Keys).Concat(Q3entry.FinancialValues.Keys), StringComparer.OrdinalIgnoreCase);
                            foreach (var key in allKeys)
                            {
                                if (key.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
                                {
                                    string statementType = Data.Data.GetStatementType(key).ToLowerInvariant();
                                    bool isCashFlow = statementType.Contains("cashflow");
                                    if (isCashFlow)
                                    {
                                        string baseKey = RemoveQuarterFromElementName(key);
                                        string q1Key = AdjustElementNameForQuarter(baseKey, 1);
                                        string q2Key = AdjustElementNameForQuarter(baseKey, 2);
                                        string q3Key = AdjustElementNameForQuarter(baseKey, 3);
                                        decimal Q1_cum = ExtractNumericValue(Q1entry.FinancialValues.TryGetValue(q1Key, out var Q1_obj) ? Q1_obj : null, q1Key);
                                        decimal Q2_cum = ExtractNumericValue(Q2entry.FinancialValues.TryGetValue(q2Key, out var Q2_obj) ? Q2_obj : null, q2Key);
                                        decimal Q3_cum = ExtractNumericValue(Q3entry.FinancialValues.TryGetValue(q3Key, out var Q3_obj) ? Q3_obj : null, q3Key);
                                        if (Q2_cum == 0 || Q3_cum == 0)
                                        {
                                            Console.WriteLine($"[WARNING] Invalid cumulative value for {baseKey}. Q2_cum: {Q2_cum}, Q3_cum: {Q3_cum}. Skipping adjustment.");
                                            continue;
                                        }
                                        decimal Q2_actual = Q2_cum - Q1_cum;
                                        decimal Q3_actual = Q3_cum - Q2_cum;
                                        if (Q2_actual < 0 || Q3_actual < 0)
                                        {
                                            Console.WriteLine($"[WARNING] Negative actual values for {baseKey}. Q2_actual: {Q2_actual}, Q3_actual: {Q3_actual}. Skipping.");
                                            continue;
                                        }
                                        Q2entry.FinancialValues[q2Key] = Q2_actual;
                                        Q3entry.FinancialValues[q3Key] = Q3_actual;
                                    }
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] Missing Q1, Q2, or Q3 entries for {company.companyName} ({company.symbol}). Skipping cashflow adjustments.");
                        }
                        Data.Data.AdjustAnnualReportYears(companyData); //Call the adjust method here before saving
                        var adjustedEntries = companyData.GetCompletedEntries();
                        if (adjustedEntries.Any())
                        {
                            await dataNonStatic.SaveAllEntriesToDatabaseAsync(company.companyId);
                        }
                        else
                        {
                            Console.WriteLine($"[INFO] No completed entries to save for {company.companyName} ({company.symbol}).");
                        }
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            await connection.OpenAsync();
                            using (SqlTransaction transaction = connection.BeginTransaction())
                            {
                                try
                                {
                                    await Data.Data.CalculateAndSaveQ4InDatabaseAsync(connection, transaction, company.companyId, dataNonStatic);
                                    transaction.Commit();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[ERROR] Transaction failed while calculating/saving Q4 for {company.companyName} ({company.symbol}): {ex.Message}");
                                    try
                                    {
                                        transaction.Rollback();
                                        Console.WriteLine($"Transaction rolled back for {company.companyName} ({company.symbol}).");
                                    }
                                    catch (Exception rollbackEx)
                                    {
                                        Console.WriteLine($"[ERROR] Transaction rollback failed for {company.companyName} ({company.symbol}): {rollbackEx.Message}");
                                    }
                                }
                            }
                        }
                        Console.WriteLine($"Finished processing for {company.companyName} ({company.symbol})");
                    }
                    semaphore.Dispose();
                }
                Console.WriteLine("[INFO] RunScraperForUnscrapedCompaniesAsync completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in RunScraperForUnscrapedCompaniesAsync: {ex.Message}");
            }
        }



        private static async Task<List<(int companyId, string companyName, string symbol, int cik)>> GetUnscrapedCompaniesFromDatabase()
        {
            var unscrapedCompanies = new List<(int companyId, string companyName, string symbol, int cik)>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                string query = @"
            SELECT c.CompanyID, c.CompanyName, c.CompanySymbol, c.CIK
            FROM CompaniesList c
            WHERE NOT EXISTS (
                SELECT 1 
                FROM FinancialData f 
                WHERE f.CompanyID = c.CompanyID
            )";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int companyId = reader.GetInt32(reader.GetOrdinal("CompanyID"));
                            string companyName = reader.GetString(reader.GetOrdinal("CompanyName"));
                            string symbol = reader.GetString(reader.GetOrdinal("CompanySymbol"));
                            int cik = reader.GetInt32(reader.GetOrdinal("CIK"));
                            unscrapedCompanies.Add((companyId, companyName, symbol, cik));
                        }
                    }
                }
            }
            return unscrapedCompanies;
        }
        public static async Task<List<(int companyId, string companyName, string symbol, int cik)>> GetCompaniesWithHigherCompanyIDAsync()
        {
            var companies = new List<(int companyId, string companyName, string symbol, int cik)>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Step 1: Retrieve the maximum CompanyID from FinancialData
                string maxIdQuery = @"
            SELECT ISNULL(MAX(CompanyID), 0) AS MaxCompanyID
            FROM FinancialData";

                int maxCompanyID = 0;

                using (SqlCommand maxIdCommand = new SqlCommand(maxIdQuery, connection))
                {
                    object result = await maxIdCommand.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        maxCompanyID = Convert.ToInt32(result);
                    }
                }

                // Step 2: Retrieve companies from CompaniesList with CompanyID > maxCompanyID
                string companiesQuery = @"
            SELECT c.CompanyID, c.CompanyName, c.CompanySymbol, c.CIK
            FROM CompaniesList c
            WHERE c.CompanyID > @MaxCompanyID
            ORDER BY c.CompanyID ASC";

                using (SqlCommand companiesCommand = new SqlCommand(companiesQuery, connection))
                {
                    companiesCommand.Parameters.AddWithValue("@MaxCompanyID", maxCompanyID);

                    using (SqlDataReader reader = await companiesCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int companyId = reader.GetInt32(reader.GetOrdinal("CompanyID"));
                            string companyName = reader.GetString(reader.GetOrdinal("CompanyName"));
                            string symbol = reader.GetString(reader.GetOrdinal("CompanySymbol"));
                            int cik = reader.GetInt32(reader.GetOrdinal("CIK"));

                            companies.Add((companyId, companyName, symbol, cik));
                        }
                    }
                }
            }
            return companies;
        }

        private static decimal ExtractNumericValue(object valueObj, string key)
        {
            if (valueObj == null)
            {
                Console.WriteLine($"[WARNING] Value object is null for key: {key}. Returning 0.");
                return 0;
            }   // Handle numeric types directly
            switch (valueObj)
            {
                case decimal decimalVal:
                    return decimalVal;
                case double doubleVal:
                    return (decimal)doubleVal;
                case float floatVal:
                    return (decimal)floatVal;
                case int intVal:
                    return intVal;
                case long longVal:
                    return longVal;
                case string strVal:
                    if (decimal.TryParse(strVal, out decimal result))
                        return result;
                    else
                    {
                        Console.WriteLine($"[WARNING] Failed to parse numeric value for key: {key}. ValueStr: '{strVal}'. Returning 0.");
                        return 0;
                    }
                default:
                    Console.WriteLine($"[WARNING] Unsupported type '{valueObj.GetType()}' for key: {key}. Returning 0.");
                    return 0;
            }
        }
        private static string AdjustElementNameForQuarter(string elementName, int quarter)
        {
            if (!elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
                return elementName;
            string pattern = @"^(HTML_)Report(\d{4}_)";
            string replacement = $"$1Q{quarter}Report$2";  // Insert Q{quarter} after 'HTML_' and before 'ReportYYYY_'
            string newKey = Regex.Replace(elementName, pattern, replacement, RegexOptions.IgnoreCase);
            return newKey;
        }
        private static string RemoveQuarterFromElementName(string elementName)
        {    // Updated pattern to capture 'HTML_' followed by 'Q' and a digit, then 'ReportYYYY_'
            string pattern = @"^(HTML_)Q\d(Report\d{4}_)";
            string replacement = "$1$2"; // This removes the quarter digit but keeps 'HTML_ReportYYYY_'
            return Regex.Replace(elementName, pattern, replacement, RegexOptions.IgnoreCase);
        }
        private static int GetCurrentFiscalYear(CompanyFinancialData companyData)
        {
            // Return the most recent fiscal year found in the entries
            var years = companyData.FinancialEntries.Values.Select(e => e.Year).Distinct().ToList();
            return years.Any() ? years.Max() : DateTime.Now.Year;
        }
        public static async Task<int> GetCompanyIdBySymbol(string companySymbol)
        {
            int companyId = 0;
            string query = "SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
                    var result = await command.ExecuteScalarAsync();
                    if (result != null)
                    {
                        companyId = Convert.ToInt32(result);
                    }
                }
            }
            return companyId;
        }
        public static ChromeDriver StartNewSession(bool headless = false)
        {
            ChromeOptions options = new ChromeOptions();
            options.AddArgument("--disable-gpu");
            options.AddArgument("--window-size=1920x1080");
            options.AddArgument("--no-sandbox");
            options.AddArgument("--disable-dev-shm-usage");
            options.AddArgument("--ignore-certificate-errors"); // Disable SSL verification
            options.BinaryLocation = @"C:\Users\kiera\Downloads\chrome-win64(4)\chrome-win64\chrome.exe";
            string chromeDriverPath = @"C:\Users\kiera\Downloads\chromedriver-win64(5)\chromedriver-win64\chromedriver.exe";  // Path to chromedriver.exe
            return new ChromeDriver(chromeDriverPath, options);
        }
        public static async Task FetchAndStoreCompanyData()
        {
            var companyData = await FetchCompanyDataFromSecAsync();
            await StoreCompanyDataInDatabase(companyData);
        }
        public static async Task<List<Data.Data.CompanyInfo>> FetchCompanyDataFromSecAsync()
        {
            var url = "https://www.sec.gov/files/company_tickers.json";
            var response = await HttpClientProvider.Client.GetAsync(url);
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                var companyData = JsonConvert.DeserializeObject<List<Data.Data.CompanyInfo>>(content);
                return companyData;
            }
            else
            {
                throw new Exception("Error fetching company data from SEC.");
            }
        }
        public static async Task<List<(int companyId, string companyName, string symbol, int cik)>> GetNasdaq100CompaniesFromDatabase()
        {
            var companies = new List<(int companyId, string companyName, string symbol, int cik)>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                string query = "SELECT CompanyID, CompanyName, CompanySymbol, CIK FROM CompaniesList";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int companyId = reader.GetInt32(reader.GetOrdinal("CompanyID"));
                            string companyName = reader.GetString(reader.GetOrdinal("CompanyName"));
                            string symbol = reader.GetString(reader.GetOrdinal("CompanySymbol"));
                            int cik = reader.GetInt32(reader.GetOrdinal("CIK"));
                            companies.Add((companyId, companyName, symbol, cik));
                        }
                    }
                }
            }
            return companies;
        }
        public static async Task<string?> GetInteractiveDataUrl(string filingUrl)
        {
            try
            {                // Use the HttpClient instance safely (from the Lazy<HttpClient>)
                string response = await HttpClientProvider.Client.GetStringAsync(filingUrl);
                HtmlDocument doc = new HtmlDocument();
                doc.LoadHtml(response);
                var interactiveDataNode = doc.DocumentNode.SelectSingleNode("//a[contains(text(), 'Interactive Data')]");
                if (interactiveDataNode != null)
                {
                    string interactiveDataUrl = $"https://www.sec.gov{interactiveDataNode.GetAttributeValue("href", string.Empty)}";
                    return interactiveDataUrl;
                }
                return null;
            }
            catch (Exception)
            {
                return null;
            }
        }
        public static async Task<string> GetCompanyName(int companyId)
        {
            string query = "SELECT CompanyName FROM CompaniesList WHERE CompanyID = @CompanyID";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    return (string)await command.ExecuteScalarAsync();
                }
            }
        }
        public static async Task<string> GetCompanySymbol(int companyId)
        {
            string query = "SELECT CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    return (string)await command.ExecuteScalarAsync();
                }
            }
        }
        private static bool IsEntryInQuarter(DateTime date, int year, int quarter) // NOTE NOT SURE IS THIS METHOD IS NEEDED AND SHOULDNT BE RELYING ON?
        {
            if (quarter == 0)
            {
                DateTime yearStart = new DateTime(year, 1, 1);
                DateTime yearEnd = new DateTime(year, 12, 31);
                return date >= yearStart && date <= yearEnd;
            }
            int startMonth = (quarter - 1) * 3 + 1;
            DateTime quarterStart = new DateTime(year, startMonth, 1);
            DateTime quarterEnd = quarterStart.AddMonths(3).AddDays(-1); // End of the quarter
            return date >= quarterStart && date <= quarterEnd;
        }
        public static async Task<string> GetCompanyCIK(string companySymbol)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                string query = "SELECT CIK FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
                    object result = await command.ExecuteScalarAsync();

                    if (result != null && result is int cikInt)
                    {
                        return cikInt.ToString();  // Convert the integer CIK to a string
                    }
                    else if (result != null && result is string cikString)
                    {
                        return cikString;  // In case it's already a string (though unlikely based on the database schema)
                    }
                    else
                    {
                        throw new Exception($"CIK not found for {companySymbol}.");
                    }
                }
            }
        }
    }
}


//WITH DuplicateEntries AS (
//    SELECT
//        *,
//        ROW_NUMBER() OVER (PARTITION BY Year, CompanyID, Quarter ORDER BY EndDate ASC) AS RowNum,
//        COUNT(*) OVER (PARTITION BY Year, CompanyID, Quarter) AS TotalCount
//    FROM
//        FinancialData
//    WHERE
//        Quarter = 0
//)
//UPDATE DuplicateEntries
//SET
//    Year = Year - 1
//WHERE
//    RowNum = 1
//    AND TotalCount > 1;