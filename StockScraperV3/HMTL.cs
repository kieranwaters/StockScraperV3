using DataElements;
using HtmlAgilityPack;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using SeleniumExtras.WaitHelpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Nasdaq100FinancialScraper;
using StockScraperV3;


namespace HTML
{
    public class HTML
    {
        private static List<ChromeDriver> driverPool = new List<ChromeDriver>();
        private static SemaphoreSlim driverSemaphore = new SemaphoreSlim(5, 5); // Allow up to 5 concurrent drivers

        public static async Task InitializeDriverPool()
        {
            for (int i = 0; i < 5; i++)
            {
                var driver = StartNewSession(); // Replace with your driver initialization logic
                driverPool.Add(driver);
            }
        }

        public static async Task ProcessInteractiveData(string interactiveDataUrl, string companyName, string companySymbol, bool isAnnualReport, string url, List<SqlCommand> batchedCommands)
        {
            await driverSemaphore.WaitAsync(); // Acquire a slot for a driver
            ChromeDriver driver = null;

            try
            {
                // Get an available driver from the pool
                lock (driverPool)
                {
                    driver = driverPool.FirstOrDefault(d => d != null && d.SessionId != null);
                    if (driver == null)
                    {
                        throw new InvalidOperationException("No available ChromeDriver instances in the pool.");
                    }
                }

                var dataTimer = Stopwatch.StartNew();
                bool loadedSuccessfully = false;
                int retries = 0;
                const int maxRetries = 3;
                const int retryDelay = 5000;
                DateTime? startDate = null;
                DateTime? endDate = null;
                Console.WriteLine($"[INFO] Starting processing of interactive data for {companyName} ({companySymbol})");

                while (!loadedSuccessfully && retries < maxRetries)
                {
                    try
                    {
                        driver.Navigate().GoToUrl(interactiveDataUrl);
                        var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
                        wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
                        loadedSuccessfully = true;
                        await Task.Delay(1000);  // Small delay to ensure page content is fully loaded

                        var financialStatementButtons = driver.FindElements(By.XPath("//a[starts-with(@id, 'menu_cat') and contains(text(), 'Financial Statements') and not(contains(text(), 'Notes'))]"));
                        //Console.WriteLine($"[INFO] Found {financialStatementButtons.Count} financial statement buttons for {companyName} ({companySymbol})");
                        bool isFirstReport = true;

                        foreach (var financialStatementsButton in financialStatementButtons)
                        {
                            try
                            {
                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", financialStatementsButton);
                                await Task.Delay(500);
                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", financialStatementsButton);
                                await Task.Delay(1000);

                                var accordionElements = driver.FindElements(By.XPath("//ul[@style='display: block;']//li[contains(@class, 'accordion')]//a[contains(@class, 'xbrlviewer')]"));
                                if (accordionElements.Count == 0)
                                {
                                 //   Console.WriteLine($"[WARNING] No accordion elements found under financial statement button for {companyName} ({companySymbol}).");
                                    continue;
                                }

                                foreach (var accordionElement in accordionElements)
                                {
                                    try
                                    {
                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", accordionElement);
                                        await Task.Delay(800);
                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", accordionElement);
                                        await Task.Delay(1000);

                                        var reportElement = wait.Until(ExpectedConditions.ElementIsVisible(By.XPath("//table[contains(@class, 'report')]")));
                                        string reportHtml = reportElement.GetAttribute("outerHTML");

                                        if (isFirstReport)
                                        {
                                            endDate = ExtractDateFromThTags(reportHtml);
                                            if (endDate == null)
                                            {
                                                Console.WriteLine($"[ERROR] Could not extract the end date from the first report for {companyName} ({companySymbol}).");
                                                continue;
                                            }
                                            startDate = isAnnualReport ? endDate.Value.AddMonths(-12) : endDate.Value.AddMonths(-3);
                                            isFirstReport = false;
                                        }

                                        var parsedHtmlElements = await HTML.ParseHtmlForElementsOfInterest(reportHtml, isAnnualReport, companyName, companySymbol);
                                        if (parsedHtmlElements.Count == 0)
                                        {
                                        //   Console.WriteLine($"[WARNING] No HTML elements of interest found for {companyName} ({companySymbol}).");
                                            continue;
                                        }

                                        await Data.Data.ExecuteBatch(batchedCommands);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"[ERROR] Exception during parsing HTML for {companyName} ({companySymbol}): {ex.Message}");
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ERROR] Exception clicking financial statement button for {companyName} ({companySymbol}): {ex.Message}");
                            }
                        }
                    }
                    catch (WebDriverTimeoutException ex)
                    {
                        retries++;
                        Console.WriteLine($"[ERROR] Timeout loading interactive data (retry {retries}/{maxRetries}) for {companyName} ({companySymbol}): {ex.Message}");
                        if (retries >= maxRetries)
                        {
                            Console.WriteLine($"[ERROR] Failed to load interactive data after maximum retries for {companyName} ({companySymbol}).");
                        }
                        else
                        {
                            await Task.Delay(retryDelay);
                        }
                    }
                }

                dataTimer.Stop();
             //   Console.WriteLine($"[INFO] Finished processing interactive data for {companyName} ({companySymbol}). Time taken: {dataTimer.ElapsedMilliseconds} ms.");
            }
            finally
            {
                driverSemaphore.Release(); // Release the driver slot
            }
        }

        public static ChromeDriver StartNewSession()
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

        public static async Task<string> GetElementTextWithRetry(IWebDriver driver, By locator, int retryCount = 3)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(5));
            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    var element = wait.Until(ExpectedConditions.ElementIsVisible(locator));
                    return element.Text;
                }
                catch (StaleElementReferenceException)
                {
                    if (i == retryCount - 1)
                    {
                        throw; // Rethrow if we've exhausted retries
                    }
                    await Task.Delay(500); // Add a small delay before retrying
                }
                catch (NoSuchElementException)
                {
                    throw new Exception("Element not found after retries.");
                }
            }
            return string.Empty;
        }
        public static async Task<List<string>> ParseHtmlForElementsOfInterest(string htmlContent, bool isAnnualReport, string companyName, string companySymbol)
        {
            var elements = new List<string>(); // This will store parsed elements
            try
            {
                var htmlDocument = new HtmlDocument();
                htmlDocument.LoadHtml(htmlContent);
                var tables = htmlDocument.DocumentNode.SelectNodes("//table[contains(@class, 'report')]");
                if (tables == null || tables.Count == 0)
                {
                    Console.WriteLine($"[ERROR] No tables with class 'report' found for {companyName} ({companySymbol}).");
                    return elements; // Return the empty list if no tables found
                }

                foreach (var table in tables)
                {
                    var rows = table.SelectNodes(".//tr");
                    if (rows == null)
                    {
                     //   Console.WriteLine($"[ERROR] No rows found in the table for {companyName} ({companySymbol}).");
                        continue;
                    }

                    DateTime? endDate = null;
                    var firstDateCell = table.SelectSingleNode(".//th[contains(@class, 'th')]/div"); // Attempt to extract the end date from the first date cell
                    if (firstDateCell != null)
                    {
                        string rawEndDate = firstDateCell.InnerText.Trim();
                        if (DateTime.TryParse(rawEndDate, out DateTime parsedEndDate))
                        {
                            endDate = parsedEndDate;
                            Program.globalEndDate = endDate;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else
                    {
                        continue; // Skip this table if no date was found
                    }

                    if (endDate.HasValue)
                    {
                        Program.globalStartDate = isAnnualReport ? endDate.Value.AddYears(-1) : endDate.Value.AddMonths(-3);
                    }
                    else
                    {
                        continue; // Skip this table if no end date was found
                    }

                    foreach (var row in rows) // Process the data rows
                    {
                        var cells = row.SelectNodes(".//td|.//th");
                        if (cells == null || cells.Count < 2)
                        {
                            continue;
                        }

                        var label = cells[0].InnerText.Trim();
                        var valueCell = cells.FirstOrDefault(cell => cell.Attributes["class"]?.Value.Contains("nump") == true);
                        if (valueCell != null)
                        {
                            string rawValue = valueCell.InnerText.Trim();
                            var (cleanedValue, multiplier) = CleanNumber(rawValue);
                            if (double.TryParse(cleanedValue, out double value))
                            {
                                value *= multiplier;
                            }
                            else
                            {
                                continue;
                            }

                            var elementsOfInterest = FinancialElementLists.HTMLElementsOfInterest;
                            var matchedElement = elementsOfInterest.FirstOrDefault(kvp => kvp.Key.Contains(label));
                            if (matchedElement.Key != null)
                            {
                                var (columnName, isShares, isCashFlowStatement, isBalanceSheet) = matchedElement.Value;
                                var elementNames = elementsOfInterest.Values.Select(v => v.Item1).ToArray();

                                // Step 1: Add the element label to the parsed elements list
                                elements.Add(label);

                                // Save data to the database
                                Data.Data.SaveToDatabase(columnName, value.ToString(), null, elementNames, null, isAnnualReport, companyName, companySymbol, Program.globalStartDate, Program.globalEndDate, isHtmlParsed: true);
                            }
                        }
                    }

                    // Step 2: Break after processing the first successful report
                    break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception occurred while parsing HTML for company {companyName}: {ex.Message}");
            }

            // Step 3: Return the collected elements
            return elements;
        }
        public static async Task ProcessInteractiveData(ChromeDriver driver, string interactiveDataUrl, string companyName, string companySymbol, bool isAnnualReport, string url, List<SqlCommand> batchedCommands)
        {
            var dataTimer = Stopwatch.StartNew();
            bool loadedSuccessfully = false;
            int retries = 0;
            const int maxRetries = 3;
            const int retryDelay = 5000;
            DateTime? startDate = null;
            DateTime? endDate = null;
            //Console.WriteLine($"[INFO] Starting processing of interactive data for {companyName} ({companySymbol})");

            while (!loadedSuccessfully && retries < maxRetries)
            {
                try
                {
                    driver.Navigate().GoToUrl(interactiveDataUrl);
                    var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
                    wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
                    loadedSuccessfully = true;
                    await Task.Delay(1000);  // Small delay to ensure page content is fully loaded

                    var financialStatementButtons = driver.FindElements(By.XPath("//a[starts-with(@id, 'menu_cat') and contains(text(), 'Financial Statements') and not(contains(text(), 'Notes'))]"));
                    //Console.WriteLine($"[INFO] Found {financialStatementButtons.Count} financial statement buttons for {companyName} ({companySymbol})");
                    bool isFirstReport = true;

                    foreach (var financialStatementsButton in financialStatementButtons)
                    {
                        try
                        {
                            ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", financialStatementsButton);
                            await Task.Delay(500);
                            ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", financialStatementsButton);
                            await Task.Delay(1000);

                            var accordionElements = driver.FindElements(By.XPath("//ul[@style='display: block;']//li[contains(@class, 'accordion')]//a[contains(@class, 'xbrlviewer')]"));
                            if (accordionElements.Count == 0)
                            {
                             //   Console.WriteLine($"[WARNING] No accordion elements found under financial statement button for {companyName} ({companySymbol}).");
                                continue;
                            }

                            foreach (var accordionElement in accordionElements)
                            {
                                try
                                {
                                    ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", accordionElement);
                                    await Task.Delay(800);
                                    ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", accordionElement);
                                    await Task.Delay(1000);

                                    var reportElement = wait.Until(ExpectedConditions.ElementIsVisible(By.XPath("//table[contains(@class, 'report')]")));
                                    string reportHtml = reportElement.GetAttribute("outerHTML");

                                    if (isFirstReport)
                                    {
                                        endDate = ExtractDateFromThTags(reportHtml);
                                        if (endDate == null)
                                        {
                                            Console.WriteLine($"[ERROR] Could not extract the end date from the first report for {companyName} ({companySymbol}).");
                                            continue;
                                        }
                                        startDate = isAnnualReport ? endDate.Value.AddMonths(-12) : endDate.Value.AddMonths(-3);
                                        isFirstReport = false;
                                    }

                                    var parsedHtmlElements = await HTML.ParseHtmlForElementsOfInterest(reportHtml, isAnnualReport, companyName, companySymbol);
                                    if (parsedHtmlElements.Count == 0)
                                    {
                                     //   Console.WriteLine($"[WARNING] No HTML elements of interest found for {companyName} ({companySymbol}).");
                                        continue;
                                    }

                                    await Data.Data.ExecuteBatch(batchedCommands);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[ERROR] Exception during parsing HTML for {companyName} ({companySymbol}): {ex.Message}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Exception clicking financial statement button for {companyName} ({companySymbol}): {ex.Message}");
                        }
                    }
                }
                catch (WebDriverTimeoutException ex)
                {
                    retries++;
                    Console.WriteLine($"[ERROR] Timeout loading interactive data (retry {retries}/{maxRetries}) for {companyName} ({companySymbol}): {ex.Message}");
                    if (retries >= maxRetries)
                    {
                        Console.WriteLine($"[ERROR] Failed to load interactive data after maximum retries for {companyName} ({companySymbol}).");
                    }
                    else
                    {
                        await Task.Delay(retryDelay);
                    }
                }
            }

            dataTimer.Stop();
         //   Console.WriteLine($"[INFO] Finished processing interactive data for {companyName} ({companySymbol}). Time taken: {dataTimer.ElapsedMilliseconds} ms.");
        }
        public static DateTime? ExtractDateFromThTags(string reportHtml)
        {
            var htmlDoc = new HtmlDocument();
            htmlDoc.LoadHtml(reportHtml);

            // Select all <th> elements in the document
            var thElements = htmlDoc.DocumentNode.SelectNodes("//th[@class='th']");

            if (thElements == null || thElements.Count == 0)
            {
                Console.WriteLine("[ERROR] No <th> elements found.");
                return null;
            }

            DateTime? mostRecentDate = null;

            foreach (var th in thElements)
            {
                string dateText = th.InnerText.Trim();
                // Clean up the date text (remove periods and double spaces)
                dateText = dateText.Replace(".", "").Replace("  ", " ").Trim();

                // Use the regular expression to check if the text is likely a date format
                if (IsValidDateFormat(dateText))
                {
                    if (DateTime.TryParse(dateText, out DateTime parsedDate))
                    {
                        if (!mostRecentDate.HasValue || parsedDate > mostRecentDate.Value)
                        {
                            mostRecentDate = parsedDate; // Keep the most recent date
                        }
                    }
                    else
                    {
                      //  Console.WriteLine($"[ERROR] Failed to parse date from text: {dateText}");
                    }
                }
                else
                {
                //    Console.WriteLine($"[INFO] Skipping non-date text: {dateText}");
                }
            }

            if (mostRecentDate.HasValue)
            {
              //  Console.WriteLine($"[DEBUG] Most recent date found: {mostRecentDate.Value}");
                return mostRecentDate;
            }

         //   Console.WriteLine($"[ERROR] No valid date could be extracted from <th> elements.");
            return null;
        }

        // Helper method to validate date formats using regex
        public static bool IsValidDateFormat(string text)
        {
            // Regex to match common date formats like "Dec 31, 2022", "July 2023", etc.
            string pattern = @"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s\d{1,2},?\s?\d{4}$";

            return System.Text.RegularExpressions.Regex.IsMatch(text, pattern);
        }
        public static (string cleanedValue, double multiplier) CleanNumber(string rawValue)
        {
            string cleanedValue = rawValue.Replace(",", "").Replace("$", "").Replace("(", "-").Replace(")", "").Trim();
            bool isShares = rawValue.Contains("shares", StringComparison.OrdinalIgnoreCase);
            double multiplier = isShares ? 1 : 1000000; // Adjust multiplier based on whether it’s shares or dollar values
            return (cleanedValue, multiplier);
        }
        public static async Task ReparseHtmlReports(int companyId, int periodId, string companyName, string companySymbol, List<SqlCommand> batchedCommands)
        {
            // Fetch all filings for the last 10 years, including both 10-K and 10-Q filings
            var filings = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companySymbol, "10-Q");
            filings.AddRange(await StockScraperV3.URL.GetFilingUrlsForLast10Years(companySymbol, "10-K"));

            // If no filings are found, return early
            if (!filings.Any())
            {
                return;
            }

            // Loop through the filings and process them asynchronously
            var tasks = filings.Select(async filing =>
            {
                using (var driver = StockScraperV3.URL.StartNewSession())
                {
                    // Get the interactive data URL for each filing
                    string interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(filing.url);
                    if (!string.IsNullOrEmpty(interactiveDataUrl))
                    {
                        try
                        {
                            // Navigate to the interactive data page
                            driver.Navigate().GoToUrl(interactiveDataUrl);
                            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(30)); // Increased timeout to 30 seconds for more complex pages
                            wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));

                            // Get the page source and parse for elements of interest
                            var pageSource = driver.PageSource;
                            ParseHtmlForElementsOfInterest(pageSource, false, companyName, companySymbol);
                        }
                        catch (WebDriverTimeoutException ex)
                        {
                            Console.WriteLine($"[ERROR] WebDriverTimeoutException during reparsing for {companyName} ({companySymbol}): {ex.Message}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Exception during reparsing for {companyName} ({companySymbol}): {ex.Message}");
                        }
                    }
                    else
                    {
                        // Log if no interactive data URL was found
                        Console.WriteLine($"[ERROR] No interactive data URL found for {companyName} ({companySymbol}).");
                    }
                }
            }).ToList();

            // Wait for all tasks to complete
            await Task.WhenAll(tasks);

            // Update the ParsedFullHTML column for this company and period, using batchedCommands
            await UpdateParsedFullHtmlColumn(companyId, periodId, batchedCommands);
        }

        private static async Task UpdateParsedFullHtmlColumn(int companyId, int periodId, List<SqlCommand> batchedCommands)
        {
            string query = @"
    UPDATE FinancialData
    SET ParsedFullHTML = 'Yes'
    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";

            var command = new SqlCommand(query);
            command.Parameters.AddWithValue("@CompanyID", companyId);
            command.Parameters.AddWithValue("@PeriodID", periodId);

            batchedCommands.Add(command); // Use the passed batchedCommands instead of the static Program.batchedCommands
        }
        public static bool IsRelevantPeriod(XElement context, bool isAnnualReport)// NOTE THIS EXISTS
        {
            string? startDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value;
            string? endDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value;
            string? instantStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "instant")?.Value;
            if (DateTime.TryParse(startDateStr, out DateTime startDate) && DateTime.TryParse(endDateStr, out DateTime endDate))
            {
                if (isAnnualReport)
                {
                    return Math.Abs((endDate - startDate).Days - 365) <= 20;
                }
                else
                {
                    return Math.Abs((endDate - startDate).Days - 90) <= 20;
                }
            }
            else if (DateTime.TryParse(instantStr, out DateTime instantDate))
            {
                return true;
            }
            return false;
        }
        public static (double sharesMultiplier, double dollarMultiplier) ExtractScalingFactor(HtmlDocument htmlDocument)
        {
            var scalingFactorNode = htmlDocument.DocumentNode.SelectSingleNode(
                "//strong[contains(text(), 'shares') or contains(text(), '$') or contains(text(), 'Millions') or contains(text(), 'Thousands')]");
            string scalingText = scalingFactorNode?.InnerText ?? string.Empty;
            double sharesMultiplier = 1;
            double dollarMultiplier = 1;
            if (scalingText.Contains("shares", StringComparison.OrdinalIgnoreCase))
            {
                if (scalingText.Contains("shares in Thousands", StringComparison.OrdinalIgnoreCase))
                    sharesMultiplier = 1000;
                else if (scalingText.Contains("shares in Millions", StringComparison.OrdinalIgnoreCase))
                    sharesMultiplier = 1_000_000;
            }
            if (scalingText.Contains("$", StringComparison.OrdinalIgnoreCase))
            {
                if (scalingText.Contains("$ in Thousands", StringComparison.OrdinalIgnoreCase))
                    dollarMultiplier = 1000;
                else if (scalingText.Contains("$ in Millions", StringComparison.OrdinalIgnoreCase))
                    dollarMultiplier = 1_000_000;
            }
            return (sharesMultiplier, dollarMultiplier);
        }
    }
}

//using DataElements;
//using HtmlAgilityPack;
//using OpenQA.Selenium;
//using OpenQA.Selenium.Chrome;
//using OpenQA.Selenium.Support.UI;
//using SeleniumExtras.WaitHelpers;
//using System;
//using System.Collections.Generic;
//using System.Data.SqlClient;
//using System.Diagnostics;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using System.Xml.Linq;
//using Nasdaq100FinancialScraper;
//using StockScraperV3;

//namespace HTML
//{
//    public class HTML
//    {
//        public static async Task<string> GetElementTextWithRetry(IWebDriver driver, By locator, int retryCount = 3)
//        {
//            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(5));
//            for (int i = 0; i < retryCount; i++)
//            {
//                try
//                {
//                    var element = wait.Until(ExpectedConditions.ElementIsVisible(locator));
//                    return element.Text;
//                }
//                catch (StaleElementReferenceException)
//                {
//                    if (i == retryCount - 1)
//                    {
//                        throw; // Rethrow if we've exhausted retries
//                    }
//                    await Task.Delay(500); // Add a small delay before retrying
//                }
//                catch (NoSuchElementException)
//                {
//                    throw new Exception("Element not found after retries.");
//                }
//            }
//            return string.Empty;
//        }
//        public static async Task<List<string>> ParseHtmlForElementsOfInterest(string htmlContent, bool isAnnualReport, string companyName, string companySymbol)
//        {
//            var elements = new List<string>(); // This will store parsed elements
//            try
//            {
//                var htmlDocument = new HtmlDocument();
//                htmlDocument.LoadHtml(htmlContent);
//                var tables = htmlDocument.DocumentNode.SelectNodes("//table[contains(@class, 'report')]");
//                if (tables == null || tables.Count == 0)
//                {
//                    Console.WriteLine($"[ERROR] No tables with class 'report' found for {companyName} ({companySymbol}).");
//                    return elements; // Return the empty list if no tables found
//                }

//                foreach (var table in tables)
//                {
//                    var rows = table.SelectNodes(".//tr");
//                    if (rows == null)
//                    {
//                        Console.WriteLine($"[ERROR] No rows found in the table for {companyName} ({companySymbol}).");
//                        continue;
//                    }

//                    DateTime? endDate = null;
//                    var firstDateCell = table.SelectSingleNode(".//th[contains(@class, 'th')]/div"); // Attempt to extract the end date from the first date cell
//                    if (firstDateCell != null)
//                    {
//                        string rawEndDate = firstDateCell.InnerText.Trim();
//                        if (DateTime.TryParse(rawEndDate, out DateTime parsedEndDate))
//                        {
//                            endDate = parsedEndDate;
//                            Program.globalEndDate = endDate;
//                        }
//                        else
//                        {
//                            continue;
//                        }
//                    }
//                    else
//                    {
//                        continue; // Skip this table if no date was found
//                    }

//                    if (endDate.HasValue)
//                    {
//                        Program.globalStartDate = isAnnualReport ? endDate.Value.AddYears(-1) : endDate.Value.AddMonths(-3);
//                    }
//                    else
//                    {
//                        continue; // Skip this table if no end date was found
//                    }

//                    foreach (var row in rows) // Process the data rows
//                    {
//                        var cells = row.SelectNodes(".//td|.//th");
//                        if (cells == null || cells.Count < 2)
//                        {
//                            continue;
//                        }

//                        var label = cells[0].InnerText.Trim();
//                        var valueCell = cells.FirstOrDefault(cell => cell.Attributes["class"]?.Value.Contains("nump") == true);
//                        if (valueCell != null)
//                        {
//                            string rawValue = valueCell.InnerText.Trim();
//                            var (cleanedValue, multiplier) = CleanNumber(rawValue);
//                            if (double.TryParse(cleanedValue, out double value))
//                            {
//                                value *= multiplier;
//                            }
//                            else
//                            {
//                                continue;
//                            }

//                            var elementsOfInterest = FinancialElementLists.HTMLElementsOfInterest;
//                            var matchedElement = elementsOfInterest.FirstOrDefault(kvp => kvp.Key.Contains(label));
//                            if (matchedElement.Key != null)
//                            {
//                                var (columnName, isShares, isCashFlowStatement, isBalanceSheet) = matchedElement.Value;
//                                var elementNames = elementsOfInterest.Values.Select(v => v.Item1).ToArray();

//                                // Step 1: Add the element label to the parsed elements list
//                                elements.Add(label);

//                                // Save data to the database
//                                Data.Data.SaveToDatabase(columnName, value.ToString(), null, elementNames, null, isAnnualReport, companyName, companySymbol, Program.globalStartDate, Program.globalEndDate, isHtmlParsed: true);
//                            }
//                        }
//                    }

//                    // Step 2: Break after processing the first successful report
//                    break;
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception occurred while parsing HTML for company {companyName}: {ex.Message}");
//            }

//            // Step 3: Return the collected elements
//            return elements;
//        }
//        public static async Task ProcessInteractiveData(ChromeDriver driver, string interactiveDataUrl, string companyName, string companySymbol, bool isAnnualReport, string url, List<SqlCommand> batchedCommands)
//        {
//            var dataTimer = Stopwatch.StartNew();
//            bool loadedSuccessfully = false;
//            int retries = 0;
//            const int maxRetries = 3;
//            const int retryDelay = 5000;
//            DateTime? startDate = null;
//            DateTime? endDate = null;
//            Console.WriteLine($"[INFO] Starting processing of interactive data for {companyName} ({companySymbol})");

//            // Use semaphore to control ChromeDriver instances
//            await Program.semaphore.WaitAsync();  // Assuming `Program.semaphore` is a SemaphoreSlim instance controlling concurrency
//            try
//            {
//                while (!loadedSuccessfully && retries < maxRetries)
//                {
//                    try
//                    {
//                        driver.Navigate().GoToUrl(interactiveDataUrl);
//                        var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
//                        wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
//                        loadedSuccessfully = true;
//                        await Task.Delay(1000);  // Small delay to ensure page content is fully loaded

//                        var financialStatementButtons = driver.FindElements(By.XPath("//a[starts-with(@id, 'menu_cat') and contains(text(), 'Financial Statements') and not(contains(text(), 'Notes'))]"));
//                        Console.WriteLine($"[INFO] Found {financialStatementButtons.Count} financial statement buttons for {companyName} ({companySymbol})");
//                        bool isFirstReport = true;

//                        foreach (var financialStatementsButton in financialStatementButtons)
//                        {
//                            try
//                            {
//                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", financialStatementsButton);
//                                await Task.Delay(500);
//                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", financialStatementsButton);
//                                await Task.Delay(1000);

//                                var accordionElements = driver.FindElements(By.XPath("//ul[@style='display: block;']//li[contains(@class, 'accordion')]//a[contains(@class, 'xbrlviewer')]"));
//                                if (accordionElements.Count == 0)
//                                {
//                                    Console.WriteLine($"[WARNING] No accordion elements found under financial statement button for {companyName} ({companySymbol}).");
//                                    continue;
//                                }

//                                foreach (var accordionElement in accordionElements)
//                                {
//                                    try
//                                    {
//                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", accordionElement);
//                                        await Task.Delay(800);
//                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", accordionElement);
//                                        await Task.Delay(1000);

//                                        var reportElement = wait.Until(ExpectedConditions.ElementIsVisible(By.XPath("//table[contains(@class, 'report')]")));
//                                        string reportHtml = reportElement.GetAttribute("outerHTML");

//                                        if (isFirstReport)
//                                        {
//                                            endDate = ExtractDateFromThTags(reportHtml);
//                                            if (endDate == null)
//                                            {
//                                                Console.WriteLine($"[ERROR] Could not extract the end date from the first report for {companyName} ({companySymbol}).");
//                                                continue;
//                                            }
//                                            startDate = isAnnualReport ? endDate.Value.AddMonths(-12) : endDate.Value.AddMonths(-3);
//                                            isFirstReport = false;
//                                        }

//                                        var parsedHtmlElements = await HTML.ParseHtmlForElementsOfInterest(reportHtml, isAnnualReport, companyName, companySymbol);
//                                        if (parsedHtmlElements.Count == 0)
//                                        {
//                                            Console.WriteLine($"[WARNING] No HTML elements of interest found for {companyName} ({companySymbol}).");
//                                            continue;
//                                        }

//                                        await Data.Data.ExecuteBatch(batchedCommands);
//                                    }
//                                    catch (Exception ex)
//                                    {
//                                        Console.WriteLine($"[ERROR] Exception during parsing HTML for {companyName} ({companySymbol}): {ex.Message}");
//                                    }
//                                }
//                            }
//                            catch (Exception ex)
//                            {
//                                Console.WriteLine($"[ERROR] Exception clicking financial statement button for {companyName} ({companySymbol}): {ex.Message}");
//                            }
//                        }
//                    }
//                    catch (WebDriverTimeoutException ex)
//                    {
//                        retries++;
//                        Console.WriteLine($"[ERROR] Timeout loading interactive data (retry {retries}/{maxRetries}) for {companyName} ({companySymbol}): {ex.Message}");
//                        if (retries >= maxRetries)
//                        {
//                            Console.WriteLine($"[ERROR] Failed to load interactive data after maximum retries for {companyName} ({companySymbol}).");
//                        }
//                        else
//                        {
//                            await Task.Delay(retryDelay);
//                        }
//                    }
//                }
//            }
//            finally
//            {
//                // Always quit driver and release semaphore to prevent resource leakage
//                driver?.Quit();
//                Program.semaphore.Release();
//            }

//            dataTimer.Stop();
//            Console.WriteLine($"[INFO] Finished processing interactive data for {companyName} ({companySymbol}). Time taken: {dataTimer.ElapsedMilliseconds} ms.");
//        }
//        public static DateTime? ExtractDateFromThTags(string reportHtml)
//        {
//            var htmlDoc = new HtmlDocument();
//            htmlDoc.LoadHtml(reportHtml);

//            // Select all <th> elements in the document
//            var thElements = htmlDoc.DocumentNode.SelectNodes("//th[@class='th']");

//            if (thElements == null || thElements.Count == 0)
//            {
//                Console.WriteLine("[ERROR] No <th> elements found.");
//                return null;
//            }

//            DateTime? mostRecentDate = null;

//            foreach (var th in thElements)
//            {
//                string dateText = th.InnerText.Trim();
//                // Clean up the date text (remove periods and double spaces)
//                dateText = dateText.Replace(".", "").Replace("  ", " ").Trim();

//                // Use the regular expression to check if the text is likely a date format
//                if (IsValidDateFormat(dateText))
//                {
//                    if (DateTime.TryParse(dateText, out DateTime parsedDate))
//                    {
//                        if (!mostRecentDate.HasValue || parsedDate > mostRecentDate.Value)
//                        {
//                            mostRecentDate = parsedDate; // Keep the most recent date
//                        }
//                    }
//                    else
//                    {
//                        Console.WriteLine($"[ERROR] Failed to parse date from text: {dateText}");
//                    }
//                }
//                else
//                {
//                    Console.WriteLine($"[INFO] Skipping non-date text: {dateText}");
//                }
//            }

//            if (mostRecentDate.HasValue)
//            {
//                Console.WriteLine($"[DEBUG] Most recent date found: {mostRecentDate.Value}");
//                return mostRecentDate;
//            }

//            Console.WriteLine($"[ERROR] No valid date could be extracted from <th> elements.");
//            return null;
//        }

//        // Helper method to validate date formats using regex
//        public static bool IsValidDateFormat(string text)
//        {
//            // Regex to match common date formats like "Dec 31, 2022", "July 2023", etc.
//            string pattern = @"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s\d{1,2},?\s?\d{4}$";

//            return System.Text.RegularExpressions.Regex.IsMatch(text, pattern);
//        }
//        public static (string cleanedValue, double multiplier) CleanNumber(string rawValue)
//        {
//            string cleanedValue = rawValue.Replace(",", "").Replace("$", "").Replace("(", "-").Replace(")", "").Trim();
//            bool isShares = rawValue.Contains("shares", StringComparison.OrdinalIgnoreCase);
//            double multiplier = isShares ? 1 : 1000000; // Adjust multiplier based on whether it’s shares or dollar values
//            return (cleanedValue, multiplier);
//        }
//        public static async Task ReparseHtmlReports(int companyId, int periodId, string companyName, string companySymbol, List<SqlCommand> batchedCommands)
//        {
//            // Fetch all filings for the last 10 years, including both 10-K and 10-Q filings
//            var filings = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companySymbol, "10-Q");
//            filings.AddRange(await StockScraperV3.URL.GetFilingUrlsForLast10Years(companySymbol, "10-K"));

//            // If no filings are found, return early
//            if (!filings.Any())
//            {
//                return;
//            }

//            // Loop through the filings and process them asynchronously
//            var tasks = filings.Select(async filing =>
//            {
//                using (var driver = StockScraperV3.URL.StartNewSession())
//                {
//                    // Get the interactive data URL for each filing
//                    string interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(filing.url);
//                    if (!string.IsNullOrEmpty(interactiveDataUrl))
//                    {
//                        try
//                        {
//                            // Navigate to the interactive data page
//                            driver.Navigate().GoToUrl(interactiveDataUrl);
//                            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(30)); // Increased timeout to 30 seconds for more complex pages
//                            wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));

//                            // Get the page source and parse for elements of interest
//                            var pageSource = driver.PageSource;
//                            ParseHtmlForElementsOfInterest(pageSource, false, companyName, companySymbol);
//                        }
//                        catch (WebDriverTimeoutException ex)
//                        {
//                            Console.WriteLine($"[ERROR] WebDriverTimeoutException during reparsing for {companyName} ({companySymbol}): {ex.Message}");
//                        }
//                        catch (Exception ex)
//                        {
//                            Console.WriteLine($"[ERROR] Exception during reparsing for {companyName} ({companySymbol}): {ex.Message}");
//                        }
//                    }
//                    else
//                    {
//                        // Log if no interactive data URL was found
//                        Console.WriteLine($"[ERROR] No interactive data URL found for {companyName} ({companySymbol}).");
//                    }
//                }
//            }).ToList();

//            // Wait for all tasks to complete
//            await Task.WhenAll(tasks);

//            // Update the ParsedFullHTML column for this company and period, using batchedCommands
//            await UpdateParsedFullHtmlColumn(companyId, periodId, batchedCommands);
//        }

//        private static async Task UpdateParsedFullHtmlColumn(int companyId, int periodId, List<SqlCommand> batchedCommands)
//        {
//            string query = @"
//    UPDATE FinancialData
//    SET ParsedFullHTML = 'Yes'
//    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";

//            var command = new SqlCommand(query);
//            command.Parameters.AddWithValue("@CompanyID", companyId);
//            command.Parameters.AddWithValue("@PeriodID", periodId);

//            batchedCommands.Add(command); // Use the passed batchedCommands instead of the static Program.batchedCommands
//        }


//        public static int GetQuarter(DateTime reportDate, bool isAnnualReport, DateTime financialYearStartDate) //NOTE THIS IS BEIGN RELIED UPON 2 TIMES
//        {
//            if (isAnnualReport)
//            {
//                return 0; // Annual report, no specific quarter
//            }
//            int financialYearStartMonth = financialYearStartDate.Month;

//            int monthsFromStart = (reportDate.Month - financialYearStartMonth + 12) % 12;
//            if (monthsFromStart < 3)
//                return 1; // First quarter
//            else if (monthsFromStart < 6)
//                return 2; // Second quarter
//            else if (monthsFromStart < 9)
//                return 3; // Third quarter
//            else
//                return 4; // Fourth quarter
//        }

//        public static DateTime GetFinancialYearStartDate(SqlConnection connection, SqlTransaction transaction, int companyId) //NOTE THIS ALSO EXISTS IN THE PROGRAM CLASS
//        {
//            using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
//            {
//                command.CommandText = @"
//                SELECT TOP 1 StartDate 
//                FROM FinancialData 
//                WHERE CompanyID = @CompanyID AND Quarter = '0'
//                ORDER BY StartDate ASC";
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                var result = command.ExecuteScalar();
//                if (result != null && DateTime.TryParse(result.ToString(), out DateTime startDate))
//                {
//                    return startDate;
//                }
//                else
//                {
//                    throw new Exception($"Could not retrieve the financial year start date for company ID {companyId}");
//                }
//            }
//        }

//        public static bool IsRelevantPeriod(XElement context, bool isAnnualReport)// NOTE THIS EXISTS
//        {
//            string? startDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value;
//            string? endDateStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value;
//            string? instantStr = context.Descendants().FirstOrDefault(e => e.Name.LocalName == "instant")?.Value;
//            if (DateTime.TryParse(startDateStr, out DateTime startDate) && DateTime.TryParse(endDateStr, out DateTime endDate))
//            {
//                if (isAnnualReport)
//                {
//                    return Math.Abs((endDate - startDate).Days - 365) <= 20;
//                }
//                else
//                {
//                    return Math.Abs((endDate - startDate).Days - 90) <= 20;
//                }
//            }
//            else if (DateTime.TryParse(instantStr, out DateTime instantDate))
//            {
//                return true;
//            }
//            return false;
//        }
//        public static (double sharesMultiplier, double dollarMultiplier) ExtractScalingFactor(HtmlDocument htmlDocument)
//        {
//            var scalingFactorNode = htmlDocument.DocumentNode.SelectSingleNode(
//                "//strong[contains(text(), 'shares') or contains(text(), '$') or contains(text(), 'Millions') or contains(text(), 'Thousands')]");
//            string scalingText = scalingFactorNode?.InnerText ?? string.Empty;
//            double sharesMultiplier = 1;
//            double dollarMultiplier = 1;
//            if (scalingText.Contains("shares", StringComparison.OrdinalIgnoreCase))
//            {
//                if (scalingText.Contains("shares in Thousands", StringComparison.OrdinalIgnoreCase))
//                    sharesMultiplier = 1000;
//                else if (scalingText.Contains("shares in Millions", StringComparison.OrdinalIgnoreCase))
//                    sharesMultiplier = 1_000_000;
//            }
//            if (scalingText.Contains("$", StringComparison.OrdinalIgnoreCase))
//            {
//                if (scalingText.Contains("$ in Thousands", StringComparison.OrdinalIgnoreCase))
//                    dollarMultiplier = 1000;
//                else if (scalingText.Contains("$ in Millions", StringComparison.OrdinalIgnoreCase))
//                    dollarMultiplier = 1_000_000;
//            }
//            return (sharesMultiplier, dollarMultiplier);
//        }
//    }
//}