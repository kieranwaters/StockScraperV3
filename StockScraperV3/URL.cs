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

            // Remove any non-alphanumeric characters
            var sanitized = Regex.Replace(input, @"[^a-zA-Z0-9\s]", "");

            // Convert to uppercase and replace spaces with underscores
            var normalised = sanitized.ToUpper().Replace(" ", "_");

            return normalised;
        }
        public static async Task ProcessFilings(
        ChromeDriver driver,
        List<(string url, string description)> filings,
        string companyName,
        string companySymbol,
        int companyId,
        int groupIndex,
        DataNonStatic dataNonStatic) // Accept shared DataNonStatic instance
        {
            int parsedReportsCount = 0;
            int totalReportsToParse = filings.Count(filing => filing.description.Contains("10-K") || filing.description.Contains("10-Q"));
            const int leewayDays = 15;

            foreach (var filing in filings) // Process each filing
            {
                string url = filing.url;
                string description = filing.description;
                bool isAnnualReport = description.Contains("10-K");
                bool isQuarterlyReport = description.Contains("10-Q");
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
                            if (isIxbrl)
                            {
                                await XBRL.XBRL.DownloadAndParseXbrlData(
                                    xbrlUrl,
                                    isAnnualReport,
                                    companyName,
                                    companySymbol,
                                    async (content, isAnnual, name, symbol) =>
                                    {
                                        await XBRL.XBRL.ParseInlineXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
                                    },
                                    async (content, isAnnual, name, symbol) =>
                                    {
                                        await XBRL.XBRL.ParseTraditionalXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
                                    });
                            }
                            else
                            {
                                await XBRL.XBRL.DownloadAndParseXbrlData(
                                    xbrlUrl,
                                    isAnnualReport,
                                    companyName,
                                    companySymbol,
                                    async (content, isAnnual, name, symbol) =>
                                    {
                                        await XBRL.XBRL.ParseTraditionalXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
                                    },
                                    async (content, isAnnual, name, symbol) =>
                                    {
                                        await XBRL.XBRL.ParseInlineXbrlContent(content, isAnnual, name, symbol, dataNonStatic, companyId);
                                    });
                            }
                            isXbrlParsed = true;
                        }
                        else
                        {
                            Console.WriteLine("No XBRL URL obtained");
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
                            await HTML.HTML.ProcessInteractiveData(driver, interactiveDataUrl, companyName, companySymbol, isAnnualReport, url, companyId, dataNonStatic);
                            isHtmlParsed = true;
                        }
                    }
                    catch (WebDriverException ex)
                    {
                        if (ex.Message.Contains("disconnected"))
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

                    break; // Exit retry loop if successful
                }
            }

            // After all filings have been processed, retrieve completed entries
            var completedEntries = await dataNonStatic.GetCompletedEntriesAsync(companyId);

            if (completedEntries.Count > 0)
            {
                await dataNonStatic.SaveEntriesToDatabaseAsync(companyId, completedEntries);
            }
            else
            {
                Console.WriteLine($"[INFO] No completed entries to save for {companyName} ({companySymbol}).");
            }
        }

        private static async Task ProcessFilingAsync(
     (string url, string description) filing,
     (int companyId, string companyName, string symbol, int cik) localCompany,
     ChromeDriverPool driverPool,
     DataNonStatic dataNonStatic)
        {
            ChromeDriver driver = null;
            try
            {
                driver = await driverPool.GetDriverAsync();
                int companyId = localCompany.companyId;
                await ProcessFilings(
                    driver,
                    new List<(string url, string description)> { filing },
                    localCompany.companyName,
                    localCompany.symbol,
                    companyId,
                    0,
                    dataNonStatic); // Pass shared instance
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception during filing processing for {localCompany.companyName} ({localCompany.symbol}): {ex.Message}");
                throw; // Re-throw the exception
            }
            finally
            {
                driverPool.ReturnDriver(driver); // Return driver to the pool
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
        public static async Task RunScraperAsync()
        {
            var nasdaq100Companies = await GetNasdaq100CompaniesFromDatabase();
            var driverPool = new ChromeDriverPool(5); // Initialize pool with 5 drivers
            var semaphore = new SemaphoreSlim(5); // Limit concurrency to match the driver pool size

            foreach (var company in nasdaq100Companies)
            {
                var localCompany = company;
                Console.WriteLine($"Starting processing for {localCompany.companyName} ({localCompany.symbol})");

                // Fetch filings concurrently
                var filingTasks = new[]
                {
            StockScraperV3.URL.GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-K"),
            StockScraperV3.URL.GetFilingUrlsForLast10Years(localCompany.cik.ToString(), "10-Q")
        };

                var filingsResults = await Task.WhenAll(filingTasks);
                var filings = filingsResults
                    .SelectMany(f => f)
                    .DistinctBy(f => f.url)
                    .Select(f => (f.url, f.description))
                    .ToList();

                if (!filings.Any())
                {
                    Console.WriteLine($"No filings found for {localCompany.companyName} ({localCompany.symbol})");
                    continue;
                }

                var dataNonStatic = new DataNonStatic(); // Shared DataNonStatic instance
                var chromeDriverTasks = new List<Task>();

                foreach (var filing in filings)
                {
                    // Semaphore ensures only 5 tasks run concurrently
                    await semaphore.WaitAsync();
                    chromeDriverTasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessFilingAsync(filing, localCompany, driverPool, dataNonStatic);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Exception during filing processing for {localCompany.companyName} ({localCompany.symbol}): {ex.Message}");
                        }
                        finally
                        {
                            semaphore.Release(); // Release the semaphore after task completion
                        }
                    }));
                }

                await Task.WhenAll(chromeDriverTasks);

                // Save completed entries to the database
                var completedEntries = await dataNonStatic.GetCompletedEntriesAsync(localCompany.companyId);
                if (completedEntries.Count > 0)
                {
                    try
                    {
                        await dataNonStatic.SaveEntriesToDatabaseAsync(localCompany.companyId, completedEntries);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Failed to save entries for {localCompany.companyName} ({localCompany.symbol}): {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine($"[INFO] No completed entries to save for {localCompany.companyName} ({localCompany.symbol}).");
                }

                // Handle Q4 calculations in a transactional manner
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    using (SqlTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            Data.Data.CalculateAndSaveQ4InDatabase(connection, transaction, localCompany.companyId);
                            transaction.Commit();
                            Console.WriteLine($"Transaction committed for {localCompany.companyName} ({localCompany.symbol}).");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Transaction failed for {localCompany.companyName} ({localCompany.symbol}): {ex.Message}");
                            try
                            {
                                transaction.Rollback();
                                Console.WriteLine($"Transaction rolled back for {localCompany.companyName} ({localCompany.symbol}).");
                            }
                            catch (Exception rollbackEx)
                            {
                                Console.WriteLine($"[ERROR] Rollback failed for {localCompany.companyName} ({localCompany.symbol}): {rollbackEx.Message}");
                            }
                        }
                    }
                }

                Console.WriteLine($"Finished processing for {localCompany.companyName} ({localCompany.symbol})");
            }

            driverPool.Dispose(); // Dispose of the driver pool after processing
            semaphore.Dispose(); // Dispose of the semaphore
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
            catch (Exception ex)
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
