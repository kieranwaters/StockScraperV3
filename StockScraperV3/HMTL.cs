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
        public static async Task<string> GetElementTextWithRetry(IWebDriver driver, By locator, int retryCount = 3)
        {
            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
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
        public static void ParseHtmlForElementsOfInterest(string htmlContent, bool isAnnualReport, string companyName, string companySymbol)
        {
            try
            {
                var htmlDocument = new HtmlDocument();
                htmlDocument.LoadHtml(htmlContent);

                // Find all tables with class "report"
                var tables = htmlDocument.DocumentNode.SelectNodes("//table[contains(@class, 'report')]");

                if (tables == null || tables.Count == 0)
                {
                   // Console.WriteLine("[ERROR] No tables with class 'report' found.");
                    return;
                }

                foreach (var table in tables)
                {
                    var rows = table.SelectNodes(".//tr");
                    if (rows == null)
                    {
                       // Console.WriteLine("[ERROR] No rows found in the table.");
                        continue;
                    }

                    DateTime? endDate = null;

                    // Attempt to extract the end date from the first date cell
                    var firstDateCell = table.SelectSingleNode(".//th[contains(@class, 'th')]/div");
                    if (firstDateCell != null)
                    {
                        string rawEndDate = firstDateCell.InnerText.Trim();
                        if (DateTime.TryParse(rawEndDate, out DateTime parsedEndDate))
                        {
                            endDate = parsedEndDate;
                            Program.globalEndDate = endDate;
                            //Console.WriteLine($"[INFO] Extracted end date: {Program.globalEndDate}");
                        }
                        else
                        {
                            //Console.WriteLine($"[ERROR] Failed to parse date from: {rawEndDate}");
                            continue;
                        }
                    }
                    else
                    {
                        //Console.WriteLine("[ERROR] No date cell found in the table.");
                        continue; // Skip this table if no date was found
                    }

                    // Set global start date based on whether it is an annual report
                    if (endDate.HasValue)
                    {
                        Program.globalStartDate = isAnnualReport ? endDate.Value.AddYears(-1) : endDate.Value.AddMonths(-3);
                        //Console.WriteLine($"[INFO] Calculated start date: {Program.globalStartDate}, end date: {Program.globalEndDate}");
                    }
                    else
                    {
                        continue; // Skip this table if no end date was found
                    }

                    // Process the data rows
                    foreach (var row in rows)
                    {
                        var cells = row.SelectNodes(".//td|.//th");
                        if (cells == null || cells.Count < 2)
                        {
                            //Console.WriteLine("[ERROR] Row does not contain enough cells for processing.");
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
                                //Console.WriteLine($"[INFO] Parsed value: {value} for label: {label}");
                            }
                            else
                            {
                               // Console.WriteLine($"[ERROR] Failed to parse numeric value from: {rawValue}");
                                continue;
                            }

                            var elementsOfInterest = FinancialElementLists.HTMLElementsOfInterest;
                            if (elementsOfInterest.TryGetValue(label, out var elementInfo))
                            {
                                // elementInfo is a tuple, so you can extract columnName and isShares
                                var (columnName, isShares, isCashFlowStatement, isBalanceSheet) = elementInfo;

                                var elementNames = elementsOfInterest.Values.Select(v => v.Item1).ToArray();

                                // Now columnName is defined, and you can pass it to SaveToDatabase
                                Data.Data.SaveToDatabase(columnName, value.ToString(), null, elementNames, null, isAnnualReport, companyName, companySymbol, isHtmlParsed: true);
                                //Console.WriteLine($"[INFO] Saved data for element {columnName}: {value}");
                            }
                            else
                            {
                                //Console.WriteLine($"[WARNING] Element not found in elements of interest: {label}");
                            }
                        }
                    }

                    break; // Exit after processing the first successful report
                }
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"[ERROR] Exception occurred while parsing HTML for company {companyName}: {ex.Message}");
            }
        }


        public static async Task ProcessInteractiveData(ChromeDriver driver, string interactiveDataUrl, string companyName, string companySymbol, bool isAnnualReport, string url)
        {
            var dataTimer = Stopwatch.StartNew();
            bool loadedSuccessfully = false;
            int retries = 0;
            const int maxRetries = 3;
            const int retryDelay = 10000;
            DateTime? startDate = null;
            DateTime? endDate = null;

           // Console.WriteLine($"[INFO] Starting processing of interactive data for {companyName} ({companySymbol})");

            while (!loadedSuccessfully && retries < maxRetries)
            {
                try
                {
                    driver.Navigate().GoToUrl(interactiveDataUrl);
                    var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(20));
                    wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
                    loadedSuccessfully = true;
                    await Task.Delay(2000);  // Small delay to ensure page content is fully loaded

                    // Get Financial Statement Buttons
                    var financialStatementButtons = driver.FindElements(By.XPath("//a[starts-with(@id, 'menu_cat') and contains(text(), 'Financial Statements') and not(contains(text(), 'Notes'))]"));

                    if (financialStatementButtons.Count == 0)
                    {
                        //Console.WriteLine("[ERROR] No financial statement buttons found for processing.");
                        return;
                    }

                    bool isFirstReport = true;
                    foreach (var financialStatementsButton in financialStatementButtons)
                    {
                        try
                        {
                            // Click the financial statement button
                            ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", financialStatementsButton);
                            await Task.Delay(500);
                            financialStatementsButton.Click();
                            await Task.Delay(1000);

                            // Process accordion elements under financial statements
                            var accordionElements = driver.FindElements(By.XPath("//ul[@style='display: block;']//li[contains(@class, 'accordion')]//a[contains(@class, 'xbrlviewer')]"));

                            if (accordionElements.Count == 0)
                            {
                               // Console.WriteLine("[WARNING] No accordion elements found under financial statement button.");
                                continue;
                            }

                            foreach (var accordionElement in accordionElements)
                            {
                                try
                                {
                                    if (accordionElement.Displayed && accordionElement.Enabled)
                                    {
                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", accordionElement);
                                        await Task.Delay(500);
                                        accordionElement.Click();
                                    }
                                    else
                                    {
                                       // Console.WriteLine("[WARNING] Accordion element is not clickable.");
                                        continue;
                                    }

                                    var reportElement = wait.Until(ExpectedConditions.ElementIsVisible(By.XPath("//table[contains(@class, 'report')]")));
                                    string reportHtml = reportElement.GetAttribute("outerHTML");

                                    // Extract date from first report
                                    if (isFirstReport)
                                    {
                                        endDate = ExtractDateFromHtml(reportHtml);
                                        if (endDate == null)
                                        {
                                           // Console.WriteLine("[ERROR] Could not extract the end date from the first report.");
                                            continue;
                                        }

                                        startDate = isAnnualReport ? endDate.Value.AddMonths(-12) : endDate.Value.AddMonths(-3);
                                        isFirstReport = false;
                                        //Console.WriteLine($"[INFO] Extracted start date: {startDate}, end date: {endDate} for company: {companyName}");
                                    }

                                    // Parse and save the data
                                    ParseHtmlForElementsOfInterest(reportHtml, isAnnualReport, companyName, companySymbol);
                                    await Data.Data.ExecuteBatch();
                                    //Console.WriteLine("[INFO] Parsed and saved HTML report data.");
                                }
                                catch (Exception ex)
                                {
                                    //Console.WriteLine($"[ERROR] Exception during parsing HTML: {ex.Message}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                           // Console.WriteLine($"[ERROR] Exception clicking financial statement button: {ex.Message}");
                        }
                    }
                }
                catch (WebDriverTimeoutException ex)
                {
                    retries++;
                    //Console.WriteLine($"[ERROR] Timeout loading interactive data (retry {retries}/{maxRetries}): {ex.Message}");
                    if (retries >= maxRetries)
                    {
                       // Console.WriteLine("[ERROR] Failed to load interactive data after maximum retries.");
                    }
                    else
                    {
                        await Task.Delay(retryDelay);
                    }
                }
            }

            dataTimer.Stop();
        }

        public static DateTime? ExtractDateFromHtml(string reportHtml)
        {
            var htmlDoc = new HtmlDocument();
            htmlDoc.LoadHtml(reportHtml);

            var xPaths = new List<string>
        {
            "/html/body/div[5]/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/th[1]/div", // Specific XPath
            "//*[contains(text(), '20')]",  // Generic XPath for dates
            "//th[contains(@class, 'th')]/div"  // Another potential XPath
        };

            foreach (var xpath in xPaths)
            {
                var dateNode = htmlDoc.DocumentNode.SelectSingleNode(xpath);

                if (dateNode != null)
                {
                    string dateText = dateNode.InnerText.Trim();
                    //Console.WriteLine($"[DEBUG] Trying to parse date from text: {dateText}");

                    if (DateTime.TryParse(dateText, out DateTime parsedDate))
                    {
                        //Console.WriteLine($"[INFO] Successfully parsed date: {parsedDate} using XPath: {xpath}");
                        return parsedDate;
                    }
                    else
                    {
                        //Console.WriteLine($"[ERROR] Failed to parse date from text: {dateText} using XPath: {xpath}");
                    }
                }
                else
                {
                    //Console.WriteLine($"[WARNING] No date node found using XPath: {xpath}");
                }
            }

            //Console.WriteLine("[ERROR] No valid date could be extracted from the HTML content.");
            return null;
        }

        public static (string cleanedValue, double multiplier) CleanNumber(string rawValue)
        {
            string cleanedValue = rawValue.Replace(",", "").Replace("$", "").Replace("(", "-").Replace(")", "").Trim();
            bool isShares = rawValue.Contains("shares", StringComparison.OrdinalIgnoreCase);

            double multiplier = isShares ? 1 : 1000000; // Adjust multiplier based on whether it’s shares or dollar values
            return (cleanedValue, multiplier);
        }
    
        public static async Task ReparseHtmlReports(int companyId, int periodId, int year, int quarter, string companyName, string companySymbol)
        {
            var filings = await StockScraperV3.URL.GetFilingUrlsForSpecificPeriod(companySymbol, year, quarter, "10-Q");
            filings.AddRange(await StockScraperV3.URL.GetFilingUrlsForSpecificPeriod(companySymbol, year, quarter, "10-K"));

            if (!filings.Any())
            {
                return;
            }
            var tasks = filings.Select(async filing =>
            {
                using (var driver = StockScraperV3.URL.StartNewSession())
                {
                    string interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(filing.url);
                    if (!string.IsNullOrEmpty(interactiveDataUrl))
                    {
                        try
                        {
                            driver.Navigate().GoToUrl(interactiveDataUrl);
                            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(60));
                            wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
                            var pageSource = driver.PageSource;
                            ParseHtmlForElementsOfInterest(pageSource, false, companyName, companySymbol);
                        }
                        catch (WebDriverTimeoutException ex)
                        {
                        }
                        catch (Exception ex)
                        {
                        }
                    }
                    else
                    {
                    }
                }
            }).ToList();

            await Task.WhenAll(tasks);
            await UpdateParsedFullHtmlColumn(companyId, periodId);
        }
        private static async Task UpdateParsedFullHtmlColumn(int companyId, int periodId)
        {
            string query = @"
        UPDATE FinancialData
        SET ParsedFullHTML = 'Yes'
        WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";
            var command = new SqlCommand(query);
            command.Parameters.AddWithValue("@CompanyID", companyId);
            command.Parameters.AddWithValue("@PeriodID", periodId);

            Program.batchedCommands.Add(command);
        }
        public static int GetQuarter(DateTime reportDate, bool isAnnualReport, DateTime financialYearStartDate)
        {
            if (isAnnualReport)
            {
                return 0; // Annual report, no specific quarter
            }

            // Fiscal year starts at the end of September, so October 1st is month 10
            int financialYearStartMonth = financialYearStartDate.Month;

            // Calculate months from the start of the fiscal year
            int monthsFromStart = (reportDate.Month - financialYearStartMonth + 12) % 12;
            // Determine the quarter based on the months from the start
            if (monthsFromStart < 3)
                return 1; // First quarter (October - December)
            else if (monthsFromStart < 6)
                return 2; // Second quarter (January - March)
            else if (monthsFromStart < 9)
                return 3; // Third quarter (April - June)
            else
                return 4; // Fourth quarter (July - September)
        }

        public static DateTime GetFinancialYearStartDate(SqlConnection connection, SqlTransaction transaction, int companyId)
        {
            using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
            {
                command.CommandText = @"
        SELECT TOP 1 StartDate 
        FROM FinancialData 
        WHERE CompanyID = @CompanyID AND Quarter = '0'
        ORDER BY StartDate ASC";

                command.Parameters.AddWithValue("@CompanyID", companyId);

                var result = command.ExecuteScalar();

                if (result != null && DateTime.TryParse(result.ToString(), out DateTime startDate))
                {
                    return startDate;
                }
                else
                {
                    throw new Exception($"Could not retrieve the financial year start date for company ID {companyId}");
                }
            }
        }
        
        public static bool IsRelevantPeriod(XElement context, bool isAnnualReport)
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
            // Select the node that contains the scaling information
            var scalingFactorNode = htmlDocument.DocumentNode.SelectSingleNode(
                "//strong[contains(text(), 'shares') or contains(text(), '$') or contains(text(), 'Millions') or contains(text(), 'Thousands')]");

            string scalingText = scalingFactorNode?.InnerText ?? string.Empty;

            // Default multipliers are 1
            double sharesMultiplier = 1;
            double dollarMultiplier = 1;

            // Check for share scaling
            if (scalingText.Contains("shares", StringComparison.OrdinalIgnoreCase))
            {
                if (scalingText.Contains("shares in Thousands", StringComparison.OrdinalIgnoreCase))
                    sharesMultiplier = 1000;
                else if (scalingText.Contains("shares in Millions", StringComparison.OrdinalIgnoreCase))
                    sharesMultiplier = 1_000_000;
            }

            // Check for dollar scaling
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

//namespace HTML
//{
//    public class HTML
//    {
//        public static async Task<string> GetElementTextWithRetry(IWebDriver driver, By locator, int retryCount = 3)
//        {
//            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
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
//            return string.Empty; // Default return in case no text is found (although this path should not be hit)
//        }
//        public static async Task ProcessInteractiveData(ChromeDriver driver, string interactiveDataUrl, string companyName, string companySymbol, bool isAnnualReport, string url)
//        {
//            var dataTimer = Stopwatch.StartNew(); // Timer for interactive data
//            bool loadedSuccessfully = false;
//            int retries = 0;
//            const int maxRetries = 3;
//            const int retryDelay = 10000; // Delay between retries in milliseconds
//            DateTime? startDate = null;
//            DateTime? endDate = null;
//            while (!loadedSuccessfully && retries < maxRetries)
//            {
//                try
//                {
//                    driver.Navigate().GoToUrl(interactiveDataUrl);
//                    var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(20)); // Increased wait time
//                    wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
//                    loadedSuccessfully = true;
//                    await Task.Delay(2000);  // Small delay to ensure page content is fully loaded
//                    var financialStatementsButton = driver.FindElement(By.XPath("//a[starts-with(@id, 'menu_cat') and contains(text(), 'Financial Statements')]"));
//                    if (financialStatementsButton == null)
//                    {
//                        Console.WriteLine("No 'Financial Statements' button found on the page.");
//                        return;
//                    }
//                    ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", financialStatementsButton);
//                    await Task.Delay(500); // Delay to ensure scrolling has finished
//                    financialStatementsButton.Click();
//                    await Task.Delay(1000); // Added delay to ensure the dropdown is fully expanded

//                    bool isFirstReport = true;

//                    // Find all 'accordion' elements under the expanded Financial Statements section
//                    var accordionElements = driver.FindElements(By.XPath("//ul[@style='display: block;']//li[contains(@class, 'accordion')]//a[contains(@class, 'xbrlviewer')]"));

//                    if (accordionElements.Count == 0)
//                    {
//                        Console.WriteLine("No 'accordion' elements found in the Financial Statements section.");
//                        return;
//                    }
//                    foreach (var accordionElement in accordionElements)
//                    {
//                        try
//                        {
//                            WebDriverWait wait1 = new WebDriverWait(driver, TimeSpan.FromSeconds(10));  // Increased wait time for dynamic content

//                            // Ensure the element is visible before interacting with it
//                            if (accordionElement.Displayed && accordionElement.Enabled)
//                            {
//                                // Scroll to the 'accordion' element and click it to load the report
//                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", accordionElement);
//                                await Task.Delay(500); // Delay to ensure scrolling has finished
//                                accordionElement.Click();
//                            }
//                            else
//                            {
//                                continue;
//                            }

//                            // Wait for the report table to load
//                            var reportElement = wait.Until(ExpectedConditions.ElementIsVisible(By.XPath("//table[contains(@class, 'report')]")));
//                            string reportHtml = reportElement.GetAttribute("outerHTML");

//                            // If it's the first report, extract the dates
//                            if (isFirstReport)
//                            {
//                                endDate = ExtractDateFromHtml(reportHtml);  // Implement this to extract the date from the HTML

//                                if (endDate == null)
//                                {
//                                    continue;  // Skip if we can't extract the date
//                                }

//                                // Calculate start date based on whether it's an annual or quarterly report
//                                startDate = isAnnualReport ? endDate.Value.AddMonths(-12) : endDate.Value.AddMonths(-3);
//                                // Use these dates for all subsequent reports
//                                isFirstReport = false;
//                            }

//                            // Parse and save the data using the start and end dates
//                            ParseHtmlForElementsOfInterest(reportHtml, isAnnualReport, companyName, companySymbol);
//                            await Nasdaq100FinancialScraper.Program.ExecuteBatch();
//                        }
//                        catch (NoSuchElementException)
//                        {
//                            Console.WriteLine("Failed to click or load the accordion element.");
//                        }
//                    }
//                }
//                catch (WebDriverTimeoutException ex)
//                {
//                    retries++;
//                    if (retries >= maxRetries)
//                    {
//                    }
//                    else
//                    {
//                        await Task.Delay(retryDelay); // Delay before retrying
//                    }
//                }
//            }
//            dataTimer.Stop();
//        }
//public static DateTime? ExtractDateFromHtml(string reportHtml)
//{
//    // Load the HTML document
//    var htmlDoc = new HtmlDocument();
//    htmlDoc.LoadHtml(reportHtml);

//    // Search for the first <div> element in <th> that contains the date
//    var dateNode = htmlDoc.DocumentNode.SelectSingleNode("//th[@class='th']/div[contains(text(), '20')]");

//    if (dateNode != null)
//    {
//        string dateText = dateNode.InnerText.Trim();

//        // Try to parse the extracted text as a date
//        if (DateTime.TryParse(dateText, out DateTime parsedDate))
//        {

//            return parsedDate;
//        }
//        else
//        {
//        }
//    }
//    else
//    {
//    }

//    return null; // Return null if no valid date is found
//}

//public static void ParseHtmlForElementsOfInterest(string htmlContent, bool isAnnualReport, string companyName, string companySymbol)
//{
//    try
//    {
//        var htmlDocument = new HtmlDocument();
//        htmlDocument.LoadHtml(htmlContent);

//        // Find all tables with class "report"
//        var tables = htmlDocument.DocumentNode.SelectNodes("//table[contains(@class, 'report')]");

//        if (tables == null || tables.Count == 0)
//        {

//            return;
//        }
//        foreach (var table in tables)
//        {
//            var rows = table.SelectNodes(".//tr");
//            if (rows == null)
//            {
//                continue;
//            }

//            DateTime? endDate = null;

//            var firstDateCell = table.SelectSingleNode(".//th[contains(@class, 'th')]/div");

//            if (firstDateCell != null)
//            {
//                string rawEndDate = firstDateCell.InnerText.Trim();
//                if (DateTime.TryParse(rawEndDate, out DateTime parsedEndDate))
//                {
//                    endDate = parsedEndDate;
//                    Program.globalEndDate = endDate;
//                }
//                else
//                {
//                    continue;
//                }
//            }

//            if (endDate.HasValue)
//            {
//                Program.globalStartDate = isAnnualReport ? endDate.Value.AddYears(-1) : endDate.Value.AddMonths(-3);
//            }
//            else
//            {
//                continue; // Skip this table if no date was found
//            }

//            // Process the data rows
//            foreach (var row in rows)
//            {
//                var cells = row.SelectNodes(".//td|.//th");
//                if (cells == null || cells.Count < 2) continue;

//                var label = cells[0].InnerText.Trim();
//                var valueCell = cells.FirstOrDefault(cell => cell.Attributes["class"]?.Value.Contains("nump") == true);

//                if (valueCell != null)
//                {
//                    string rawValue = valueCell.InnerText.Trim();
//                    var (cleanedValue, multiplier) = CleanNumber(rawValue);

//                    if (double.TryParse(cleanedValue, out double value))
//                    {
//                        value *= multiplier;
//                    }
//                    else
//                    {
//                        continue;
//                    }

//                    var elementsOfInterest = FinancialElementLists.HTMLElementsOfInterest;
//                    if (elementsOfInterest.TryGetValue(label, out var elementInfo))
//                    {
//                        // elementInfo is a tuple, so you can extract columnName from it
//                        var (columnName, isShares) = elementInfo;

//                        var elementNames = elementsOfInterest.Values.Select(v => v.Item1).ToArray();

//                        // Now columnName is defined, and you can pass it to SaveToDatabase
//                        Program.SaveToDatabase(columnName, value.ToString(), null, elementNames, null, isAnnualReport, companyName, companySymbol, isHtmlParsed: true);
//                    }
//                }
//            }

//            break; // Exit after processing the first successful report
//        }
//    }
//    catch (Exception ex)
//    {
//    }
//}
//        public static async Task ReparseHtmlReports(int companyId, int periodId, int year, int quarter, string companyName, string companySymbol)
//        {
//            var filings = await StockScraperV3.URL.GetFilingUrlsForSpecificPeriod(companySymbol, year, quarter, "10-Q");
//            filings.AddRange(await StockScraperV3.URL.GetFilingUrlsForSpecificPeriod(companySymbol, year, quarter, "10-K"));

//            if (!filings.Any())
//            {
//                return;
//            }
//            var tasks = filings.Select(async filing =>
//            {
//                using (var driver = StockScraperV3.URL.StartNewSession())
//                {
//                    string interactiveDataUrl = await StockScraperV3.URL.GetInteractiveDataUrl(filing.url);
//                    if (!string.IsNullOrEmpty(interactiveDataUrl))
//                    {
//                        try
//                        {
//                            driver.Navigate().GoToUrl(interactiveDataUrl);
//                            var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(60));
//                            wait.Until(driver => ((IJavaScriptExecutor)driver).ExecuteScript("return document.readyState").Equals("complete"));
//                            var pageSource = driver.PageSource;
//                            ParseHtmlForElementsOfInterest(pageSource, false, companyName, companySymbol);
//                        }
//                        catch (WebDriverTimeoutException ex)
//                        {
//                        }
//                        catch (Exception ex)
//                        {
//                        }
//                    }
//                    else
//                    {
//                    }
//                }
//            }).ToList();

//            await Task.WhenAll(tasks);
//            await UpdateParsedFullHtmlColumn(companyId, periodId);
//        }
//        private static async Task UpdateParsedFullHtmlColumn(int companyId, int periodId)
//        {
//            string query = @"
//        UPDATE FinancialData
//        SET ParsedFullHTML = 'Yes'
//        WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";
//            var command = new SqlCommand(query);
//            command.Parameters.AddWithValue("@CompanyID", companyId);
//            command.Parameters.AddWithValue("@PeriodID", periodId);

//            Program.batchedCommands.Add(command);
//        }
//        public static int GetQuarter(DateTime reportDate, bool isAnnualReport, DateTime financialYearStartDate)
//        {
//            if (isAnnualReport)
//            {
//                return 0; // Annual report, no specific quarter
//            }

//            // Fiscal year starts at the end of September, so October 1st is month 10
//            int financialYearStartMonth = financialYearStartDate.Month;

//            // Calculate months from the start of the fiscal year
//            int monthsFromStart = (reportDate.Month - financialYearStartMonth + 12) % 12;
//            // Determine the quarter based on the months from the start
//            if (monthsFromStart < 3)
//                return 1; // First quarter (October - December)
//            else if (monthsFromStart < 6)
//                return 2; // Second quarter (January - March)
//            else if (monthsFromStart < 9)
//                return 3; // Third quarter (April - June)
//            else
//                return 4; // Fourth quarter (July - September)
//        }

//        public static DateTime GetFinancialYearStartDate(SqlConnection connection, SqlTransaction transaction, int companyId)
//        {
//            using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
//            {
//                command.CommandText = @"
//        SELECT TOP 1 StartDate 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND Quarter = '0'
//        ORDER BY StartDate ASC";

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


//        public static bool IsRelevantPeriod(XElement context, bool isAnnualReport)
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
//        public static (string cleanedValue, double multiplier) CleanNumber(string rawValue)
//        {
//            string cleanedValue = rawValue.Replace(",", "").Replace("$", "").Replace("(", "-").Replace(")", "").Trim();
//            bool isShares = rawValue.Contains("shares", StringComparison.OrdinalIgnoreCase);

//            double multiplier = isShares ? 1 : 1000000; // Adjust multiplier based on whether it’s shares or dollar values
//            return (cleanedValue, multiplier);
//        }


//        public static (double sharesMultiplier, double dollarMultiplier) ExtractScalingFactor(HtmlDocument htmlDocument)
//        {
//            // Select the node that contains the scaling information
//            var scalingFactorNode = htmlDocument.DocumentNode.SelectSingleNode(
//                "//strong[contains(text(), 'shares') or contains(text(), '$') or contains(text(), 'Millions') or contains(text(), 'Thousands')]");

//            string scalingText = scalingFactorNode?.InnerText ?? string.Empty;

//            // Default multipliers are 1
//            double sharesMultiplier = 1;
//            double dollarMultiplier = 1;

//            // Check for share scaling
//            if (scalingText.Contains("shares", StringComparison.OrdinalIgnoreCase))
//            {
//                if (scalingText.Contains("shares in Thousands", StringComparison.OrdinalIgnoreCase))
//                    sharesMultiplier = 1000;
//                else if (scalingText.Contains("shares in Millions", StringComparison.OrdinalIgnoreCase))
//                    sharesMultiplier = 1_000_000;
//            }

//            // Check for dollar scaling
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