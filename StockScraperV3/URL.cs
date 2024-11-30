using HtmlAgilityPack;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium;
using System.Data.SqlClient;
using System.Xml.Linq;
using Nasdaq100FinancialScraper;
using OpenQA.Selenium.Support.UI;
using System.Diagnostics;
using Newtonsoft.Json;
using static Nasdaq100FinancialScraper.Program;
using DataElements;
using System.Linq;
using OpenQA.Selenium.BiDi.Communication;
using HTML;

namespace StockScraperV3
{    public class URL
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string connectionString = "Server=DESKTOP-SI08RN8\\SQLEXPRESS;Database=StockDataScraperDatabase;Integrated Security=True;";
        static URL()
        {
            httpClient.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
            httpClient.DefaultRequestHeaders.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            httpClient.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.5");
            httpClient.DefaultRequestHeaders.Add("Connection", "keep-alive");
        }
        public static async Task<ChromeDriver> ProcessFilings(ChromeDriver driver, List<(string url, string description)> filings, string companyName, string companySymbol, int companyId, int groupIndex, List<SqlCommand> batchedCommands, SemaphoreSlim semaphore)
        {
            int parsedReportsCount = 0;
            int totalReportsToParse = filings.Count(filing => filing.description.Contains("10-K") || filing.description.Contains("10-Q"));
            var parsedElements = new HashSet<string>();
            var expectedElements = FinancialElementLists.HTMLElementsOfInterest.Keys.ToList();
            const int leewayDays = 15;
            foreach (var filing in filings)
            { 
                string url = filing.url;
                string description = filing.description;
                bool isAnnualReport = description.Contains("10-K");
                bool isQuarterlyReport = description.Contains("10-Q");
                bool hasLoggedQuarterInfo = false; // New flag to ensure logging happens only once per report
                DateTime? localStartDate = null;
                DateTime? localEndDate = null;
                bool isHtmlParsed = false;
                bool isXbrlParsed = false;
                int retries = 3;
                for (int attempt = 0; attempt < retries; attempt++)
                {
                        try
                        {
                            string? xbrlUrl = await XBRL.XBRL.GetXbrlUrl(url);
                            if (!string.IsNullOrEmpty(xbrlUrl))
                            {
                                await XBRL.XBRL.DownloadAndParseXbrlData(
                                    xbrlUrl,
                                    isAnnualReport,
                                    companyName,
                                    companySymbol,
                                    async (content, annual, name, symbol) => await XBRL.XBRL.ParseTraditionalXbrlContent(content, annual, name, symbol),
                                    async (content, annual, name, symbol) => await XBRL.XBRL.ParseInlineXbrlContent(content, annual, name, symbol));
                                isXbrlParsed = true;
                                localEndDate = Program.globalEndDate;
                                localStartDate = Program.globalStartDate;
                            }
                        }
                        catch (Exception ex)
                        {
                            if (attempt == retries - 1) throw;
                            continue; // Retry
                        }
                        // Interactive HTML Data Processing
                        bool allElementsParsed = false;
                        try
                        {
                            string? interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(url);
                            if (!string.IsNullOrEmpty(interactiveDataUrl))
                            {
                                if (driver == null || driver.SessionId == null)// Ensure driver is active
                                {
                                    await semaphore.WaitAsync();
                                    try
                                    {
                                        driver = StartNewSession();
                                    }
                                    catch (Exception ex)
                                    {
                                        semaphore.Release();
                                        Console.WriteLine($"[ERROR] Failed to create ChromeDriver: {ex.Message}");
                                        throw;
                                    }
                                }
                                await HTML.HTML.ProcessInteractiveData(driver, interactiveDataUrl, companyName, companySymbol, isAnnualReport, url, batchedCommands);
                            }
                        }
                        catch (WebDriverException ex)
                        {
                            if (ex.Message.Contains("disconnected"))
                            {
                               Console.WriteLine($"[ERROR] ChromeDriver disconnected for {companyName}. Attempt {attempt + 1} of {retries}.");
                                driver?.Quit();
                                semaphore.Release();
                                await semaphore.WaitAsync();
                                try
                                {
                                    driver = StartNewSession();
                                }
                                catch (Exception e)
                                {
                                    semaphore.Release();
                                    Console.WriteLine($"[ERROR] Failed to recreate ChromeDriver: {e.Message}");
                                    throw;
                                }
                                continue; // Retry
                            }
                            else
                            {
                                throw;
                            }
                        }
                        catch (Exception ex)
                        {
                            if (attempt == retries - 1) throw;
                            continue; // Retry
                        }
                    if (localEndDate.HasValue)
                    {
                        try
                        {
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                await connection.OpenAsync();
                                using (var transaction = connection.BeginTransaction())
                                {
                                    // Get fiscal year end date from Data class
                                    DateTime fiscalYearEnd = Data.Data.GetFiscalYearEndForSpecificYear(companyId, localEndDate.Value.Year, connection, transaction);

                                    // Determine fiscal year start date from the fiscal year end date
                                    DateTime fiscalYearStart = fiscalYearEnd.AddYears(-1).AddDays(1);

                                    // Calculate the quarter using the Data class method
                                    int quarter = Data.Data.CalculateQuarterByFiscalDayMonth(companyId, localEndDate.Value, connection, transaction, leewayDays);

                                    if (isQuarterlyReport)
                                    {
                                        Data.Data.SaveQuarterData(companyId, localEndDate.Value, quarter, "QuarterlyReport", 0, isHtmlParsed, isXbrlParsed, batchedCommands);
                                    }
                                    else
                                    {
                                        Data.Data.SaveQuarterData(companyId, localEndDate.Value, 0, "AnnualReport", 0, isHtmlParsed, isXbrlParsed, batchedCommands);
                                    }

                                    transaction.Commit();
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Failed to save data for {companyName}: {ex.Message}");
                        }

                        parsedReportsCount++;
                        if (parsedReportsCount == totalReportsToParse)
                        {
                            try
                            {
                                using (SqlConnection connection = new SqlConnection(connectionString))
                                {
                                    await connection.OpenAsync();
                                    Data.Data.CalculateQ4InDatabase(connection, companyId);
                                    Data.Data.CalculateAndSaveQ4InDatabase(connection, companyId);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ERROR] Failed to calculate Q4 data for {companyName}: {ex.Message}");
                            }
                        }
                    }
                }
            }
            await Data.Data.ExecuteBatch(batchedCommands);
            //Console.WriteLine($"[INFO] Finished processing all filings for {companyName} ({companySymbol})");
            return driver;
        }
        //public static async Task<ChromeDriver> ProcessFilings(ChromeDriver driver, List<(string url, string description)> filings, string companyName, string companySymbol, int companyId, int groupIndex, List<SqlCommand> batchedCommands, SemaphoreSlim semaphore)
        //{
        //    int parsedReportsCount = 0;
        //    int totalReportsToParse = filings.Count(filing => filing.description.Contains("10-K") || filing.description.Contains("10-Q"));
        //    var parsedElements = new HashSet<string>();
        //    var expectedElements = FinancialElementLists.HTMLElementsOfInterest.Keys.ToList();
        //    foreach (var filing in filings)
        //    {
        //        string url = filing.url;
        //        string description = filing.description;
        //        bool isAnnualReport = description.Contains("10-K");
        //        bool isQuarterlyReport = description.Contains("10-Q");
        //        bool hasLoggedQuarterInfo = false; // New flag to ensure logging happens only once per report
        //        DateTime? localStartDate = null;
        //        DateTime? localEndDate = null;
        //        bool isHtmlParsed = false;
        //        bool isXbrlParsed = false;
        //        int retries = 3;
        //        for (int attempt = 0; attempt < retries; attempt++)
        //        {
        //            try
        //            {
        //                string? xbrlUrl = await XBRL.XBRL.GetXbrlUrl(url);
        //                if (!string.IsNullOrEmpty(xbrlUrl))
        //                {
        //                    await XBRL.XBRL.DownloadAndParseXbrlData(
        //                        xbrlUrl,
        //                        isAnnualReport,
        //                        companyName,
        //                        companySymbol,
        //                        async (content, annual, name, symbol) => await XBRL.XBRL.ParseTraditionalXbrlContent(content, annual, name, symbol),
        //                        async (content, annual, name, symbol) => await XBRL.XBRL.ParseInlineXbrlContent(content, annual, name, symbol));
        //                    isXbrlParsed = true;
        //                    localEndDate = Program.globalEndDate;
        //                    localStartDate = Program.globalStartDate;
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                if (attempt == retries - 1) throw;
        //                continue; // Retry
        //            }
        //            // Interactive HTML Data Processing
        //            bool allElementsParsed = false;
        //            try
        //            {
        //                string? interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(url);
        //                if (!string.IsNullOrEmpty(interactiveDataUrl))
        //                {
        //                    if (driver == null || driver.SessionId == null)// Ensure driver is active
        //                    {
        //                        await semaphore.WaitAsync();
        //                        try
        //                        {
        //                            driver = StartNewSession();
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            semaphore.Release();
        //                            Console.WriteLine($"[ERROR] Failed to create ChromeDriver: {ex.Message}");
        //                            throw;
        //                        }
        //                    }
        //                    await HTML.HTML.ProcessInteractiveData(driver, interactiveDataUrl, companyName, companySymbol, isAnnualReport, url, batchedCommands);
        //                }
        //            }
        //            catch (WebDriverException ex)
        //            {
        //                if (ex.Message.Contains("disconnected"))
        //                {
        //                    Console.WriteLine($"[ERROR] ChromeDriver disconnected for {companyName}. Attempt {attempt + 1} of {retries}.");
        //                    driver?.Quit();
        //                    semaphore.Release();
        //                    await semaphore.WaitAsync();
        //                    try
        //                    {
        //                        driver = StartNewSession();
        //                    }
        //                    catch (Exception e)
        //                    {
        //                        semaphore.Release();
        //                        Console.WriteLine($"[ERROR] Failed to recreate ChromeDriver: {e.Message}");
        //                        throw;
        //                    }
        //                    continue; // Retry
        //                }
        //                else
        //                {
        //                    throw;
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                if (attempt == retries - 1) throw;
        //                continue; // Retry
        //            }
        //            if (localEndDate.HasValue)// Save data to the database
        //            {
        //                int quarter = 0;
        //                DateTime? fiscalYearStartDate = null;
        //                try
        //                {
        //                    using (SqlConnection connection = new SqlConnection(connectionString))
        //                    {
        //                        await connection.OpenAsync();
        //                        using (var transaction = connection.BeginTransaction())
        //                        {
        //                            if (isQuarterlyReport)// Step to retrieve the fiscal year start date for quarterly report
        //                            {
        //                                var command = new SqlCommand(@"
        //                SELECT TOP 1 StartDate 
        //                FROM FinancialData 
        //                WHERE CompanyID = @CompanyID 
        //                AND Quarter = 0  -- Annual reports only
        //                ORDER BY EndDate DESC", connection, transaction);
        //                                command.Parameters.AddWithValue("@CompanyID", companyId);
        //                                fiscalYearStartDate = (DateTime?)await command.ExecuteScalarAsync();
        //                                if (fiscalYearStartDate == null)
        //                                {
        //                                    continue; // Skip to the next filing
        //                                }
        //                                fiscalYearStartDate = new DateTime(fiscalYearStartDate.Value.Year, fiscalYearStartDate.Value.Month, fiscalYearStartDate.Value.Day);
        //                                Data.Data.SaveQuarterData(companyId, localEndDate.Value, quarter, "QuarterlyReport", 0, isHtmlParsed, isXbrlParsed, batchedCommands);
        //                            }
        //                            else
        //                            {
        //                                Data.Data.SaveQuarterData(companyId, localEndDate.Value, 0, "AnnualReport", 0, isHtmlParsed, isXbrlParsed, batchedCommands);
        //                            }
        //                            transaction.Commit();
        //                        }
        //                    }
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"[ERROR] Failed to save data for {companyName}: {ex.Message}");
        //                }
        //                parsedReportsCount++;
        //                if (parsedReportsCount == totalReportsToParse)
        //                {
        //                    try
        //                    {
        //                        using (SqlConnection connection = new SqlConnection(connectionString))
        //                        {
        //                            await connection.OpenAsync();
        //                            Data.Data.CalculateQ4InDatabase(connection, companyId);
        //                            Data.Data.CalculateAndSaveQ4InDatabase(connection, companyId);
        //                        }
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        Console.WriteLine($"[ERROR] Failed to calculate Q4 data for {companyName}: {ex.Message}");
        //                    }
        //                }
        //                break;
        //            }
        //        }
        //    }
        //    await Data.Data.ExecuteBatch(batchedCommands);
        //    //Console.WriteLine($"[INFO] Finished processing all filings for {companyName} ({companySymbol})");
        //    return driver;
        //}
        private static readonly Lazy<HttpClient> lazyHttpClient = new Lazy<HttpClient>(() => CreateHttpClient());
        private static async Task ProcessFilingAsync((string url, string description) filing, (int companyId, string companyName, string symbol, int cik) localCompany, SemaphoreSlim semaphore)
        {
            ChromeDriver driver = null;
            var localBatchedCommands = new List<SqlCommand>();
            try
            {
                // Start a new ChromeDriver session
                driver = StartNewSession();
                int companyId = localCompany.companyId;

                // Process the filing
                await ProcessFilings(driver, new List<(string url, string description)> { filing }, localCompany.companyName, localCompany.symbol, companyId, 0, localBatchedCommands, semaphore);
            }
            finally
            {
                // Execute batched SQL commands
                await Data.Data.ExecuteBatch(localBatchedCommands);

                // Quit the driver to free up resources
                driver?.Quit();

                // Release the semaphore slot
                semaphore.Release();
            }
        }


        // Property to get the static HttpClient
        private static HttpClient HttpClientInstance => lazyHttpClient.Value;

        // Method to create the HttpClient instance with the correct headers
        private static HttpClient CreateHttpClient()
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
            client.DefaultRequestHeaders.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            client.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.5");
            client.DefaultRequestHeaders.Add("Connection", "keep-alive");
            return client;
        }
        public static async Task<List<(string url, string description)>> GetFilingUrlsForLast10Years(string companyCIK, string filingType)
        {
            string url = $"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={companyCIK}&type={filingType}&count=100&output=atom";
            List<(string url, string description)> filings = new List<(string url, string description)>();

            int retries = 3;
            int delayMilliseconds = 5000;

            for (int i = 0; i < retries; i++)
            {
                try
                {
                    // Delay between requests
                    await Task.Delay(delayMilliseconds);

                    // Make the HTTP request
                    string response = await httpClient.GetStringAsync(url);

                    // Parse the XML response and extract the relevant data
                    XNamespace atom = "http://www.w3.org/2005/Atom";
                    filings = XDocument.Parse(response)
                        .Descendants(atom + "entry")
                        .Where(entry => DateTime.Parse(entry.Element(atom + "updated")?.Value ?? DateTime.MinValue.ToString()).Year >= DateTime.Now.Year - 10)
                        .Select(entry => (
                            url: entry.Element(atom + "link")?.Attribute("href")?.Value!,
                            description: entry.Element(atom + "title")?.Value ?? string.Empty
                        ))
                        .ToList();

                    break; // Exit loop after successful request
                }
                catch (HttpRequestException ex) when ((int)ex.StatusCode == 429)
                {
                    // Handle 429 Too Many Requests response by waiting before retrying
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
        public static async Task RunScraperAsync()
        {
            var nasdaq100Companies = await GetNasdaq100CompaniesFromDatabase();

            // Create a semaphore with a maximum count of 5
            var semaphore = new SemaphoreSlim(5);

            // Process one company at a time
            foreach (var company in nasdaq100Companies)
            {
                var localCompany = company;
                Console.WriteLine($"Starting processing for {localCompany.companyName} ({localCompany.symbol})");

                var filingTasks = new[]
                {
            await GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-K"),
            await GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-Q")
        };

                var filings = filingTasks
                    .SelectMany(f => f)
                    .DistinctBy(f => f.url)
                    .ToList();

                if (!filings.Any())
                {
                    Console.WriteLine($"No filings found for {localCompany.companyName} ({localCompany.symbol})");
                    continue;
                }

                var chromeDriverTasks = new List<Task>();

                foreach (var filing in filings)
                {
                    // Wait for an available slot in the semaphore
                    await semaphore.WaitAsync();

                    // Start a task to process the filing using the new method
                    var filingTask = ProcessFilingAsync(filing, localCompany, semaphore);
                    chromeDriverTasks.Add(filingTask);
                }

                // Wait for all filings of the current company to be processed
                await Task.WhenAll(chromeDriverTasks);

                Console.WriteLine($"Finished processing for {localCompany.companyName} ({localCompany.symbol})");
            }
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
        public static ChromeDriver StartNewSession(bool headless = true)
        {
            ChromeOptions options = new ChromeOptions();
            options.AddArgument("--disable-gpu");
            options.AddArgument("--window-size=1920x1080");
            options.AddArgument("--no-sandbox");
            options.AddArgument("--disable-dev-shm-usage");
            options.AddArgument("--ignore-certificate-errors"); // Disable SSL verification
            options.BinaryLocation = @"C:\Users\kiera\Downloads\chrome-win64\chrome-win64\chrome.exe";
            string chromeDriverPath = @"C:\Users\kiera\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe";  // Path to chromedriver.exe
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
            var response = await httpClient.GetAsync(url);

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
            {
                // Use the HttpClient instance safely (from the Lazy<HttpClient>)
                string response = await HttpClientInstance.GetStringAsync(filingUrl);

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
            catch (Exception ex)
            {
      //          Console.WriteLine($"[ERROR] Failed to retrieve Interactive Data URL: {ex.Message}");
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

//using HtmlAgilityPack;
//using OpenQA.Selenium.Chrome;
//using OpenQA.Selenium;
//using System.Data.SqlClient;
//using System.Xml.Linq;
//using Nasdaq100FinancialScraper;
//using OpenQA.Selenium.Support.UI;
//using System.Diagnostics;
//using Newtonsoft.Json;
//using static Nasdaq100FinancialScraper.Program;
//using DataElements;
//using System.Linq;
//using OpenQA.Selenium.BiDi.Communication;
//using HTML;

//namespace StockScraperV3
//{
//    public class URL
//    {
//        private static readonly HttpClient httpClient = new HttpClient();
//        private static readonly string connectionString = "Server=DESKTOP-SI08RN8\\SQLEXPRESS;Database=StockDataScraperDatabase;Integrated Security=True;";
//        static URL()
//        {
//            httpClient.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
//            httpClient.DefaultRequestHeaders.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
//            httpClient.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.5");
//            httpClient.DefaultRequestHeaders.Add("Connection", "keep-alive");
//        }
//        public static async Task<ChromeDriver> ProcessFilings(
//    ChromeDriver driver,
//    List<(string url, string description)> filings,
//    string companyName,
//    string companySymbol,
//    int companyId,
//    int groupIndex,
//    List<SqlCommand> batchedCommands,
//    SemaphoreSlim semaphore)
//        {
//            int parsedReportsCount = 0;
//            int totalReportsToParse = filings.Count(filing => filing.description.Contains("10-K") || filing.description.Contains("10-Q"));

//            var parsedElements = new HashSet<string>();
//            var expectedElements = FinancialElementLists.HTMLElementsOfInterest.Keys.ToList();

//            Console.WriteLine($"[INFO] Total reports to parse for {companyName}: {totalReportsToParse}");

//            foreach (var filing in filings)
//            {
//                var filingTimer = Stopwatch.StartNew();
//                string url = filing.url;
//                string description = filing.description;
//                bool isAnnualReport = description.Contains("10-K");
//                bool isQuarterlyReport = description.Contains("10-Q");

//                DateTime? localStartDate = null;
//                DateTime? localEndDate = null;
//                bool isHtmlParsed = false;
//                bool isXbrlParsed = false;

//                int retries = 3;
//                Console.WriteLine($"[INFO] Starting processing for filing: {url}");

//                for (int attempt = 0; attempt < retries; attempt++)
//                {
//                    try
//                    {
//                        // XBRL Report Processing
//                        try
//                        {
//                            string? xbrlUrl = await XBRL.XBRL.GetXbrlUrl(url);
//                            if (!string.IsNullOrEmpty(xbrlUrl))
//                            {
//                                Console.WriteLine($"[INFO] Processing XBRL data for: {description}");

//                                await XBRL.XBRL.DownloadAndParseXbrlData(
//                                    xbrlUrl,
//                                    isAnnualReport,
//                                    companyName,
//                                    companySymbol,
//                                    async (content, annual, name, symbol) => await XBRL.XBRL.ParseTraditionalXbrlContent(content, annual, name, symbol),
//                                    async (content, annual, name, symbol) => await XBRL.XBRL.ParseInlineXbrlContent(content, annual, name, symbol));

//                                isXbrlParsed = true;
//                                localEndDate = Program.globalEndDate;
//                                localStartDate = Program.globalStartDate;

//                                if (!localEndDate.HasValue)
//                                {
//                                    Console.WriteLine($"[ERROR] localEndDate is not set after processing XBRL for {description} for {companyName} ({companySymbol})");
//                                }
//                            }
//                        }
//                        catch (Exception ex)
//                        {
//                            Console.WriteLine($"[ERROR] Exception during XBRL processing for {companyName}: {ex.Message}");
//                            if (attempt == retries - 1) throw;
//                            continue; // Retry
//                        }

//                        // Interactive HTML Data Processing
//                        bool allElementsParsed = false;

//                        try
//                        {
//                            string? interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(url);
//                            if (!string.IsNullOrEmpty(interactiveDataUrl))
//                            {
//                                Console.WriteLine($"[INFO] Attempting to load Interactive Data for: {interactiveDataUrl}");

//                                await HTML.HTML.ProcessInteractiveData(driver, interactiveDataUrl, companyName, companySymbol, isAnnualReport, url, batchedCommands);
//                                allElementsParsed = true;
//                            }
//                            else
//                            {
//                                Console.WriteLine($"[WARNING] No interactive data URL found for {companyName} ({companySymbol}).");
//                            }
//                        }
//                        catch (WebDriverException ex)
//                        {
//                            if (ex.Message.Contains("disconnected"))
//                            {
//                                Console.WriteLine($"[ERROR] ChromeDriver disconnected for {companyName}. Attempt {attempt + 1} of {retries}.");

//                                driver?.Quit();
//                                semaphore.Release();

//                                // Wait on semaphore before creating a new driver
//                                await semaphore.WaitAsync();
//                                try
//                                {
//                                    driver = StartNewSession(); // Recreate driver
//                                    Console.WriteLine("[INFO] Recreated ChromeDriver instance after disconnection.");
//                                }
//                                catch (Exception e)
//                                {
//                                    semaphore.Release();
//                                    Console.WriteLine($"[ERROR] Failed to recreate ChromeDriver: {e.Message}");
//                                    throw;
//                                }

//                                continue; // Retry
//                            }
//                            else
//                            {
//                                throw;
//                            }
//                        }
//                        catch (Exception ex)
//                        {
//                            Console.WriteLine($"[ERROR] Exception during HTML processing for {companyName}: {ex.Message}");
//                            if (attempt == retries - 1) throw;
//                            continue; // Retry
//                        }

//                        if (!allElementsParsed)
//                        {
//                            Console.WriteLine($"[ERROR] Not all elements were parsed successfully for {companyName} ({companySymbol}). Retrying...");
//                            continue; // Retry
//                        }

//                        // Save data to the database
//                        if (localEndDate.HasValue)
//                        {
//                            int quarter = 0;

//                            try
//                            {
//                                using (SqlConnection connection = new SqlConnection(connectionString))
//                                {
//                                    await connection.OpenAsync();
//                                    using (var transaction = connection.BeginTransaction())
//                                    {
//                                        if (isQuarterlyReport)
//                                        {
//                                            quarter = Data.Data.GetQuarterFromEndDate(localEndDate.Value, companyId, connection, transaction);
//                                            Data.Data.SaveQuarterData(companyId, localEndDate.Value, quarter, "QuarterlyReport", 0, isHtmlParsed, isXbrlParsed, batchedCommands);
//                                        }
//                                        else
//                                        {
//                                            Data.Data.SaveQuarterData(companyId, localEndDate.Value, 0, "AnnualReport", 0, isHtmlParsed, isXbrlParsed, batchedCommands);
//                                        }
//                                        transaction.Commit();
//                                    }
//                                }
//                            }
//                            catch (Exception ex)
//                            {
//                                Console.WriteLine($"[ERROR] Failed to save data for {companyName}: {ex.Message}");
//                            }
//                        }

//                        parsedReportsCount++;
//                        if (parsedReportsCount == totalReportsToParse)
//                        {
//                            try
//                            {
//                                using (SqlConnection connection = new SqlConnection(connectionString))
//                                {
//                                    await connection.OpenAsync();
//                                    Data.Data.CalculateQ4InDatabase(connection, companyId);
//                                    Data.Data.CalculateAndSaveQ4InDatabase(connection, companyId);
//                                }
//                            }
//                            catch (Exception ex)
//                            {
//                                Console.WriteLine($"[ERROR] Failed to calculate Q4 data for {companyName}: {ex.Message}");
//                            }
//                        }

//                        filingTimer.Stop();
//                        Console.WriteLine($"[INFO] Finished processing for filing: {url}, Time taken: {filingTimer.ElapsedMilliseconds} ms.");

//                        // Success, break out of the retry loop
//                        break;
//                    }
//                    catch (Exception ex)
//                    {
//                        Console.WriteLine($"[ERROR] Exception during filing processing for {companyName}: {ex.Message}");
//                        if (attempt == retries - 1)
//                        {
//                            Console.WriteLine($"[ERROR] Maximum retries reached for {companyName} on filing {url}. Skipping to next filing.");
//                        }
//                        else
//                        {
//                            Console.WriteLine($"[INFO] Retrying processing for {companyName}, attempt {attempt + 1} of {retries}.");
//                        }
//                    }
//                }
//            }

//            await Data.Data.ExecuteBatch(batchedCommands);
//            Console.WriteLine($"[INFO] Finished processing all filings for {companyName} ({companySymbol})");

//            return driver;
//        }
//        private static readonly Lazy<HttpClient> lazyHttpClient = new Lazy<HttpClient>(() => CreateHttpClient());
//        private static async Task ProcessFilingAsync((string url, string description) filing, (int companyId, string companyName, string symbol, int cik) localCompany, SemaphoreSlim semaphore)
//        {
//            ChromeDriver driver = null;
//            var localBatchedCommands = new List<SqlCommand>();
//            try
//            {
//                // Start a new ChromeDriver session
//                driver = StartNewSession();
//                int companyId = localCompany.companyId;

//                // Process the filing
//                await ProcessFilings(driver, new List<(string url, string description)> { filing }, localCompany.companyName, localCompany.symbol, companyId, 0, localBatchedCommands, semaphore);
//            }
//            finally
//            {
//                // Execute batched SQL commands
//                await Data.Data.ExecuteBatch(localBatchedCommands);

//                // Quit the driver to free up resources
//                driver?.Quit();

//                // Release the semaphore slot
//                semaphore.Release();
//            }
//        }


//        // Property to get the static HttpClient
//        private static HttpClient HttpClientInstance => lazyHttpClient.Value;

//        // Method to create the HttpClient instance with the correct headers
//        private static HttpClient CreateHttpClient()
//        {
//            var client = new HttpClient();
//            client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
//            client.DefaultRequestHeaders.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
//            client.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.5");
//            client.DefaultRequestHeaders.Add("Connection", "keep-alive");
//            return client;
//        }
//        public static async Task<List<(string url, string description)>> GetFilingUrlsForLast10Years(string companyCIK, string filingType)
//        {
//            string url = $"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={companyCIK}&type={filingType}&count=100&output=atom";
//            List<(string url, string description)> filings = new List<(string url, string description)>();

//            int retries = 3;
//            int delayMilliseconds = 5000;

//            for (int i = 0; i < retries; i++)
//            {
//                try
//                {
//                    // Delay between requests
//                    await Task.Delay(delayMilliseconds);

//                    // Make the HTTP request
//                    string response = await httpClient.GetStringAsync(url);

//                    // Parse the XML response and extract the relevant data
//                    XNamespace atom = "http://www.w3.org/2005/Atom";
//                    filings = XDocument.Parse(response)
//                        .Descendants(atom + "entry")
//                        .Where(entry => DateTime.Parse(entry.Element(atom + "updated")?.Value ?? DateTime.MinValue.ToString()).Year >= DateTime.Now.Year - 10)
//                        .Select(entry => (
//                            url: entry.Element(atom + "link")?.Attribute("href")?.Value!,
//                            description: entry.Element(atom + "title")?.Value ?? string.Empty
//                        ))
//                        .ToList();

//                    break; // Exit loop after successful request
//                }
//                catch (HttpRequestException ex) when ((int)ex.StatusCode == 429)
//                {
//                    // Handle 429 Too Many Requests response by waiting before retrying
//                    await Task.Delay(delayMilliseconds * (i + 1)); // Exponential back-off
//                }
//                catch (Exception ex)
//                {
//                    Console.WriteLine($"[ERROR] Failed to retrieve filings: {ex.Message}");
//                    break; // Exit the loop if another error occurs
//                }
//            }

//            return filings;
//        }
//        public static async Task RunScraperAsync()
//        {
//            var nasdaq100Companies = await GetNasdaq100CompaniesFromDatabase();

//            // Create a semaphore with a maximum count of 5
//            var semaphore = new SemaphoreSlim(5);

//            // Process one company at a time
//            foreach (var company in nasdaq100Companies)
//            {
//                var localCompany = company;
//                Console.WriteLine($"Starting processing for {localCompany.companyName} ({localCompany.symbol})");

//                var filingTasks = new[]
//                {
//            await GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-K"),
//            await GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-Q")
//        };

//                var filings = filingTasks
//                    .SelectMany(f => f)
//                    .DistinctBy(f => f.url)
//                    .ToList();

//                if (!filings.Any())
//                {
//                    Console.WriteLine($"No filings found for {localCompany.companyName} ({localCompany.symbol})");
//                    continue;
//                }

//                var chromeDriverTasks = new List<Task>();

//                foreach (var filing in filings)
//                {
//                    // Wait for an available slot in the semaphore
//                    await semaphore.WaitAsync();

//                    // Start a task to process the filing using the new method
//                    var filingTask = ProcessFilingAsync(filing, localCompany, semaphore);
//                    chromeDriverTasks.Add(filingTask);
//                }

//                // Wait for all filings of the current company to be processed
//                await Task.WhenAll(chromeDriverTasks);

//                Console.WriteLine($"Finished processing for {localCompany.companyName} ({localCompany.symbol})");
//            }
//        }
//        public static async Task<int> GetCompanyIdBySymbol(string companySymbol)
//        {
//            int companyId = 0;
//            string query = "SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                using (SqlCommand command = new SqlCommand(query, connection))
//                {
//                    command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
//                    var result = await command.ExecuteScalarAsync();
//                    if (result != null)
//                    {
//                        companyId = Convert.ToInt32(result);
//                    }
//                }
//            }
//            return companyId;
//        }
//        public static ChromeDriver StartNewSession(bool headless = true)
//        {
//            ChromeOptions options = new ChromeOptions();
//            options.AddArgument("--disable-gpu");
//            options.AddArgument("--window-size=1920x1080");
//            options.AddArgument("--no-sandbox");
//            options.AddArgument("--disable-dev-shm-usage");
//            options.AddArgument("--ignore-certificate-errors"); // Disable SSL verification
//            options.BinaryLocation = @"C:\Users\kiera\Downloads\chrome-win64\chrome-win64\chrome.exe";
//            string chromeDriverPath = @"C:\Users\kiera\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe";  // Path to chromedriver.exe
//            return new ChromeDriver(chromeDriverPath, options);

//        }


//        public static async Task FetchAndStoreCompanyData()
//        {
//            var companyData = await FetchCompanyDataFromSecAsync();
//            await StoreCompanyDataInDatabase(companyData);
//        }
//        public static async Task<List<Data.Data.CompanyInfo>> FetchCompanyDataFromSecAsync()
//        {
//            var url = "https://www.sec.gov/files/company_tickers.json";
//            var response = await httpClient.GetAsync(url);

//            if (response.IsSuccessStatusCode)
//            {
//                var content = await response.Content.ReadAsStringAsync();
//                var companyData = JsonConvert.DeserializeObject<List<Data.Data.CompanyInfo>>(content);
//                return companyData;
//            }
//            else
//            {
//                throw new Exception("Error fetching company data from SEC.");
//            }
//        }
//        public static async Task<List<(int companyId, string companyName, string symbol, int cik)>> GetNasdaq100CompaniesFromDatabase()
//        {
//            var companies = new List<(int companyId, string companyName, string symbol, int cik)>();
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                string query = "SELECT CompanyID, CompanyName, CompanySymbol, CIK FROM CompaniesList";
//                using (SqlCommand command = new SqlCommand(query, connection))
//                {
//                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                    {
//                        while (await reader.ReadAsync())
//                        {
//                            int companyId = reader.GetInt32(reader.GetOrdinal("CompanyID"));
//                            string companyName = reader.GetString(reader.GetOrdinal("CompanyName"));
//                            string symbol = reader.GetString(reader.GetOrdinal("CompanySymbol"));
//                            int cik = reader.GetInt32(reader.GetOrdinal("CIK"));
//                            companies.Add((companyId, companyName, symbol, cik));
//                        }
//                    }
//                }
//            }
//            return companies;
//        }

//        public static async Task<string?> GetInteractiveDataUrl(string filingUrl)
//        {
//            try
//            {
//                // Use the HttpClient instance safely (from the Lazy<HttpClient>)
//                string response = await HttpClientInstance.GetStringAsync(filingUrl);

//                HtmlDocument doc = new HtmlDocument();
//                doc.LoadHtml(response);
//                var interactiveDataNode = doc.DocumentNode.SelectSingleNode("//a[contains(text(), 'Interactive Data')]");
//                if (interactiveDataNode != null)
//                {
//                    string interactiveDataUrl = $"https://www.sec.gov{interactiveDataNode.GetAttributeValue("href", string.Empty)}";
//                    return interactiveDataUrl;
//                }
//                return null;
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Failed to retrieve Interactive Data URL: {ex.Message}");
//                return null;
//            }
//        }
//        public static async Task<string> GetCompanyName(int companyId)
//        {
//            string query = "SELECT CompanyName FROM CompaniesList WHERE CompanyID = @CompanyID";
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                using (SqlCommand command = new SqlCommand(query, connection))
//                {
//                    command.Parameters.AddWithValue("@CompanyID", companyId);
//                    return (string)await command.ExecuteScalarAsync();
//                }
//            }
//        }
//        public static async Task<string> GetCompanySymbol(int companyId)
//        {
//            string query = "SELECT CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                using (SqlCommand command = new SqlCommand(query, connection))
//                {
//                    command.Parameters.AddWithValue("@CompanyID", companyId);
//                    return (string)await command.ExecuteScalarAsync();
//                }
//            }
//        }
//        private static bool IsEntryInQuarter(DateTime date, int year, int quarter) // NOTE NOT SURE IS THIS METHOD IS NEEDED AND SHOULDNT BE RELYING ON?
//        {
//            if (quarter == 0)
//            {
//                DateTime yearStart = new DateTime(year, 1, 1);
//                DateTime yearEnd = new DateTime(year, 12, 31);
//                return date >= yearStart && date <= yearEnd;
//            }
//            int startMonth = (quarter - 1) * 3 + 1;
//            DateTime quarterStart = new DateTime(year, startMonth, 1);
//            DateTime quarterEnd = quarterStart.AddMonths(3).AddDays(-1); // End of the quarter

//            return date >= quarterStart && date <= quarterEnd;
//        }

//        public static async Task<string> GetCompanyCIK(string companySymbol)
//        {
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                string query = "SELECT CIK FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
//                using (SqlCommand command = new SqlCommand(query, connection))
//                {
//                    command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
//                    object result = await command.ExecuteScalarAsync();

//                    if (result != null && result is int cikInt)
//                    {
//                        return cikInt.ToString();  // Convert the integer CIK to a string
//                    }
//                    else if (result != null && result is string cikString)
//                    {
//                        return cikString;  // In case it's already a string (though unlikely based on the database schema)
//                    }
//                    else
//                    {
//                        throw new Exception($"CIK not found for {companySymbol}.");
//                    }
//                }
//            }
//        }
//    }
//}