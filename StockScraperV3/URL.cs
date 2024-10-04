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

namespace StockScraperV3
{    public class URL
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string connectionString = "Server=LAPTOP-871MLHAT\\sqlexpress;Database=StockDataScraperDatabase;Integrated Security=True;";
        //public static DateTime? globalStartDate = null;
        //public static DateTime? globalEndDate = null;
        //public static DateTime? globalInstantDate = null;
        static URL()
        {
            httpClient.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
        }
        public static async Task RunScraperAsync()
        {
            var nasdaq100Companies = await GetNasdaq100CompaniesFromDatabase();
            foreach (var company in nasdaq100Companies)
            {
                //Console.WriteLine($"Starting processing for {company.companyName} ({company.symbol})");
                var filingTasks = new[]
                {
                    GetFilingUrlsForLast10Years(company.cik.ToString(), "10-K"),
                    GetFilingUrlsForLast10Years(company.cik.ToString(), "10-Q")
                };
                var filings = (await Task.WhenAll(filingTasks))
                    .SelectMany(f => f) // Flatten the resulting lists
                    .ToList();
                if (!filings.Any())
                {
                    //Console.WriteLine($"No filings found for {company.companyName} ({company.symbol})");
                    continue; // Skip the company if no filings are found
                }
                ChromeDriver driver = null;
                try
                {
                    driver = StartNewSession();
                    await ProcessFilings(driver, filings, company.companyName, company.symbol);
                }
                catch (WebDriverException ex)
                {
                    if (ex.Message.Contains("disconnected"))
                    {
                        //Console.WriteLine("ChromeDriver disconnected. Restarting the session...");
                        driver?.Quit();
                        driver = StartNewSession();
                        await ProcessFilings(driver, filings, company.companyName, company.symbol);
                    }
                    else
                    {
                        //Console.WriteLine($"WebDriver error: {ex.Message}");
                        throw;
                    }
                }
                finally
                {
                    driver?.Quit(); // Ensure ChromeDriver is properly quit in all scenarios
                }
            }
        }
        public static async Task ProcessFilings(ChromeDriver driver, List<(string url, string description)> filings, string companyName, string companySymbol)
        {
            // Initialize a counter for tracking parsed reports (quarterly/annual)
            int parsedReportsCount = 0;
            int totalReportsToParse = filings.Count(filing => filing.description.Contains("10-K") || filing.description.Contains("10-Q"));

            int companyId;
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand("SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol", connection))
                {
                    command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
                    companyId = (int)command.ExecuteScalar();
                }
            }

            foreach (var filing in filings)
            {
                var filingTimer = Stopwatch.StartNew(); // Timer for individual filings
                string url = filing.url;
                string description = filing.description;
                bool isAnnualReport = description.Contains("10-K");  // Check if this is an annual report (10-K)
                bool isQuarterlyReport = description.Contains("10-Q");  // Check if this is a quarterly report (10-Q)

                bool isHtmlParsed = false;
                bool isXbrlParsed = false;

                // XBRL Report Processing
                string? xbrlUrl = await XBRL.XBRL.GetXbrlUrl(url);
                if (!string.IsNullOrEmpty(xbrlUrl))
                {
                    Program.globalStartDate = null;
                    Program.globalEndDate = null;
                    Program.globalInstantDate = null;

                    await XBRL.XBRL.DownloadAndParseXbrlData(
                        xbrlUrl,
                        isAnnualReport,
                        companyName,
                        companySymbol,
                        async (content, annual, name, symbol) => await XBRL.XBRL.ParseTraditionalXbrlContent(content, annual, name, symbol),
                        async (content, annual, name, symbol) => await XBRL.XBRL.ParseInlineXbrlContent(content, annual, name, symbol)
                    );

                    // Set isXbrlParsed to true since XBRL was processed successfully
                    isXbrlParsed = true;

                    // Batch Execution
                    await Data.Data.ExecuteBatch();

                    if (!globalEndDate.HasValue)
                    {
                        //Console.WriteLine($"[ERROR] globalEndDate is not set after processing XBRL for {description} for {companyName} ({companySymbol})");
                    }
                }

                // Interactive HTML Data Processing
                string? interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(url);
                if (!string.IsNullOrEmpty(interactiveDataUrl))
                {
                    await HTML.HTML.ProcessInteractiveData(driver, interactiveDataUrl, companyName, companySymbol, isAnnualReport, url);

                    // Set isHtmlParsed to true since HTML was processed successfully
                    isHtmlParsed = true;

                    if (!globalEndDate.HasValue)
                    {
                        //Console.WriteLine($"[ERROR] globalEndDate is not set after HTML processing for {companyName} ({companySymbol})");
                    }
                }
                else
                {
                    string htmlContent = await XBRL.XBRL.GetHtmlContent(url);
                    HTML.HTML.ParseHtmlForElementsOfInterest(htmlContent, isAnnualReport, companyName, companySymbol);
                    await Data.Data.ExecuteBatch();

                    // Set isHtmlParsed to true if fallback HTML parsing was performed
                    isHtmlParsed = true;

                    if (!globalEndDate.HasValue)
                    {
                        //Console.WriteLine($"[ERROR] globalEndDate is not set after fallback HTML parsing for {companyName} ({companySymbol})");
                    }
                }

                // Processing Annual or Quarterly Report
                if (globalEndDate.HasValue)
                {
                    int quarter = 0;  // Default quarter for annual reports

                    if (isQuarterlyReport)
                    {
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            using (var transaction = connection.BeginTransaction())
                            {
                                try
                                {
                                    quarter = Data.Data.GetQuarterFromEndDate(globalEndDate.Value, companyId, connection, transaction);
                                    Data.Data.SaveQuarterData(companyId, globalEndDate.Value, quarter, "QuarterlyReport", 0, isHtmlParsed, isXbrlParsed);
                                    transaction.Commit();
                                }
                                catch (Exception ex)
                                {
                                    transaction.Rollback();
                                    Console.WriteLine($"[ERROR] Transaction rolled back due to error: {ex.Message}");
                                }
                            }
                        }
                    }
                    else
                    {
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            using (var transaction = connection.BeginTransaction())
                            {
                                try
                                {
                                    Data.Data.SaveQuarterData(companyId, globalEndDate.Value, 0, "AnnualReport", 0, isHtmlParsed, isXbrlParsed);
                                    transaction.Commit();
                                }
                                catch (Exception ex)
                                {
                                    transaction.Rollback();
                                    Console.WriteLine($"[ERROR] Transaction rolled back due to error: {ex.Message}");
                                }
                            }
                        }
                    }
                }

                // Increment parsed reports count
                parsedReportsCount++;
                // After parsing the last report for the company, trigger Q4 calculation
                if (parsedReportsCount == totalReportsToParse)
                {
                    Console.WriteLine($"[INFO] All reports parsed for {companyName} ({companySymbol}). Triggering Q4 calculation.");

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        // Call both Q4 calculation methods
                        Data.Data.CalculateQ4InDatabase(connection, companyId);  // For ElementsOfInterest
                        Data.Data.CalculateAndSaveQ4InDatabase(connection, companyId);  // For HTMLElementsOfInterest
                    }

                    Console.WriteLine($"[INFO] Q4 calculation triggered for {companyName} ({companySymbol})");
                }

                filingTimer.Stop();
            }

            await Data.Data.ExecuteBatch();  // Final batch execution after all filings
            Console.WriteLine($"[INFO] Finished processing all filings for {companyName} ({companySymbol})");
        }






        public static async Task FetchAndStoreCompanyData()
        {
            var companyData = await FetchCompanyDataFromSecAsync();
            await StoreCompanyDataInDatabase(companyData);
            //Console.WriteLine("Fetched and stored company data successfully.");
        }
        public static async Task<List<Data.Data.CompanyInfo>> FetchCompanyDataFromSecAsync()
        {
            var url = "https://www.sec.gov/files/company_tickers.json";
            var response = await httpClient.GetAsync(url);

            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                //Console.WriteLine("API Response: " + content); // Log the response
                var companyData = JsonConvert.DeserializeObject<List<Data.Data.CompanyInfo>>(content);
                return companyData;
            }
            else
            {
                //Console.WriteLine("API Error: " + response.StatusCode); // Log the error status code
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
        public static async Task<List<(string url, string description)>> GetFilingUrlsForLast10Years(string companyCIK, string filingType)
        {
            string url = $"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={companyCIK}&type={filingType}&count=100&output=atom";
            //Console.WriteLine($"Fetching filings from URL: {url}");

            // Initialize the list of filings
            List<(string url, string description)> filings = new List<(string url, string description)>();

            int retries = 3; // Maximum retries in case of a 429 response
            int delayMilliseconds = 5000; // Increase to 5 seconds between retries

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

                    //Console.WriteLine($"{filings.Count} filings found for CIK {companyCIK}, Type: {filingType}");

                    // If we successfully fetched the filings, break the loop
                    break;
                }
                catch (HttpRequestException ex) when ((int)ex.StatusCode == 429)
                {
                    // Handle the 429 Too Many Requests response by waiting before retrying
                    //Console.WriteLine("Received 429 Too Many Requests. Waiting before retrying...");
                    await Task.Delay(delayMilliseconds * (i + 1)); // Exponential back-off (wait longer each retry)
                }
                catch (Exception ex)
                {
                    // Handle other exceptions
                    //Console.WriteLine($"Error fetching filings: {ex.Message}");
                    break; // Exit the loop if another error occurs
                }
            }

            return filings;
        }


        public static async Task<string?> GetInteractiveDataUrl(string filingUrl)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
                    string response = await client.GetStringAsync(filingUrl);
                    HtmlDocument doc = new HtmlDocument();
                    doc.LoadHtml(response);

                    var interactiveDataNode = doc.DocumentNode.SelectSingleNode("//a[contains(text(), 'Interactive Data')]");
                    if (interactiveDataNode != null)
                    {
                        string interactiveDataUrl = $"https://www.sec.gov{interactiveDataNode.GetAttributeValue("href", string.Empty)}";
                        //Console.WriteLine($"Interactive Data URL found: {interactiveDataUrl}");
                        return interactiveDataUrl;
                    }

                    //Console.WriteLine($"No Interactive Data URL found for filing: {filingUrl}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"Error fetching interactive data URL: {ex.Message}");
                return null;
            }
        }
        public static ChromeDriver StartNewSession(bool headless = true)
        {
            ChromeOptions options = new ChromeOptions();
            //if (headless)
            //{
            //    options.AddArgument("--headless");
            //}
            options.AddArgument("--disable-gpu");
            options.AddArgument("--window-size=1920x1080");
            options.AddArgument("--no-sandbox");
            options.AddArgument("--disable-dev-shm-usage");
            options.BinaryLocation = @"C:\Users\kiera\Downloads\chrome-win64\chrome-win64\chrome.exe";
            string chromeDriverPath = @"C:\Users\kiera\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe";  // Path to chromedriver.exe
            return new ChromeDriver(chromeDriverPath, options);
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
        public static async Task<List<(string url, string description)>> GetFilingUrlsForSpecificPeriod(string companySymbol, int year, int quarter, string filingType)
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
                client.DefaultRequestHeaders.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
                client.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.5");
                client.DefaultRequestHeaders.Add("Connection", "keep-alive");

                string cik = await GetCompanyCIK(companySymbol);
                string url = $"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filingType}&dateb={year}0101&owner=exclude&count=100&output=atom";
                //Console.WriteLine($"Fetching filings for {companySymbol}, Year: {year}, Quarter: {quarter}, Type: {filingType}, URL: {url}");

                string response = await client.GetStringAsync(url);
                XNamespace atom = "http://www.w3.org/2005/Atom";
                var filings = XDocument.Parse(response)
    .Descendants(atom + "entry")
    .Where(entry =>
    {
        DateTime updatedDate = DateTime.Parse(entry.Element(atom + "updated")?.Value ?? DateTime.MinValue.ToString());

        // Pre-filter: if the date is way outside the target year, skip it
        if (updatedDate.Year < year - 1 || updatedDate.Year > year + 1)
        {
            return false;
        }

        // Use the more detailed IsEntryInQuarter for remaining entries
        return IsEntryInQuarter(updatedDate, year, quarter);
    })
    .Select(entry => (
        url: entry.Element(atom + "link")?.Attribute("href")?.Value!,
        description: entry.Element(atom + "title")?.Value ?? string.Empty
    ))
    .ToList();


                //Console.WriteLine($"{filings.Count} filings found for {companySymbol}, Year: {year}, Quarter: {quarter}");
                return filings;
            }
        }
        private static bool IsEntryInQuarter(DateTime date, int year, int quarter)
        {
            if (quarter == 0)
            {
                DateTime yearStart = new DateTime(year, 1, 1);
                DateTime yearEnd = new DateTime(year, 12, 31);
                Console.WriteLine($"Checking if date {date.ToShortDateString()} is within the year {year} (Annual Report)");
                return date >= yearStart && date <= yearEnd;
            }
            int startMonth = (quarter - 1) * 3 + 1;
            DateTime quarterStart = new DateTime(year, startMonth, 1);
            DateTime quarterEnd = quarterStart.AddMonths(3).AddDays(-1); // End of the quarter
            //Console.WriteLine($"Checking if date {date.ToShortDateString()} is within {quarterStart.ToShortDateString()} and {quarterEnd.ToShortDateString()}");
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
                        //Console.WriteLine($"Found CIK for {companySymbol}: {cikInt}");
                        return cikInt.ToString();  // Convert the integer CIK to a string
                    }
                    else if (result != null && result is string cikString)
                    {
                        //Console.WriteLine($"Found CIK for {companySymbol}: {cikString}");
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