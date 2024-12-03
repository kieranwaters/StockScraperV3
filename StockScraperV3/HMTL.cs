using HtmlAgilityPack;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using SeleniumExtras.WaitHelpers;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Data;

namespace HTML
{
    public class HTML
    {
        private static List<ChromeDriver> driverPool = new List<ChromeDriver>();
        private static SemaphoreSlim driverSemaphore = new SemaphoreSlim(5, 5); // Allow up to 5 concurrent drivers
        private static readonly Dictionary<string, string> normalizedStatementTypeMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "operations", "Statement_of_Operations" },
            { "statements of operations", "Statement_of_Operations" },
            { "statement of operations", "Statement_of_Operations" },
            { "cashflow", "Cashflow_Statement" },
            { "cash flow statement", "Cashflow_Statement" },
            { "income statement", "Income_Statement" },
            { "balance sheet", "Balance_Sheet" },
        };
        private static string ConstructHtmlElementName(int quarter, int year, string statementType, string elementLabel)
        {
            string sanitizedStatementType = Regex.Replace(statementType, @"[\s:()]", "");
            string sanitizedElementLabel = Regex.Replace(elementLabel, @"[\s:()]", "");
            sanitizedElementLabel = sanitizedElementLabel.Replace(":", "");
            return $"HTML_Q{quarter}Report{year}_{sanitizedStatementType}_{sanitizedElementLabel}";
        }
        public static async Task InitializeDriverPool()
        {
            for (int i = 0; i < 5; i++)
            {
                var driver = StartNewSession(); // Replace with your driver initialization logic
                driverPool.Add(driver);
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
        public static DateTime AdjustToNearestQuarterEndDate(DateTime date)
        {  // Define the possible quarter end dates for the given year
            DateTime q1End = new DateTime(date.Year, 3, 31);
            DateTime q2End = new DateTime(date.Year, 6, 30);
            DateTime q3End = new DateTime(date.Year, 9, 30);
            DateTime q4End = new DateTime(date.Year, 12, 31);  // Calculate the difference in days between the date and each quarter end
            var quarterEnds = new List<DateTime> { q1End, q2End, q3End, q4End };
            var closestQuarterEnd = quarterEnds.OrderBy(q => Math.Abs((q - date).TotalDays)).First();
            return closestQuarterEnd;
        }
        public static async Task ProcessInteractiveData(ChromeDriver driver, string interactiveDataUrl, string companyName, string companySymbol, bool isAnnualReport, string filingUrl, int companyId, DataNonStatic dataNonStatic)
        {
            var parsedEntries = new List<FinancialDataEntry>(); // List to accumulate parsed entries
            await driverSemaphore.WaitAsync();
            try
            {
                var dataTimer = Stopwatch.StartNew();
                bool loadedSuccessfully = false;
                int retries = 0;
                const int maxRetries = 3;
                const int retryDelay = 5000;
                while (!loadedSuccessfully && retries < maxRetries)
                {
                    try
                    {
                        driver.Navigate().GoToUrl(interactiveDataUrl);
                        var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
                        wait.Until(d => ((IJavaScriptExecutor)d).ExecuteScript("return document.readyState").Equals("complete"));
                        loadedSuccessfully = true;
                        await Task.Delay(1000);  // Small delay to ensure page content is fully loaded
                        var financialStatementButtons = driver.FindElements(By.XPath("//a[starts-with(@id, 'menu_cat') and contains(text(), 'Financial Statements') and not(contains(text(), 'Notes'))]"));
                        bool isFirstReport = true;
                        foreach (var financialStatementsButton in financialStatementButtons)
                        {
                            try
                            {
                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", financialStatementsButton);
                                await Task.Delay(300);
                                ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", financialStatementsButton);
                                await Task.Delay(400);
                                var accordionElements = driver.FindElements(By.XPath("//ul[@style='display: block;']//li[contains(@class, 'accordion')]//a[contains(@class, 'xbrlviewer')]"));
                                if (accordionElements.Count == 0)
                                {
                                    continue;
                                }
                                foreach (var accordionElement in accordionElements)
                                {
                                    try
                                    { // Extract the statement name from the button text
                                        string statementName = accordionElement.Text.Trim();
                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(true);", accordionElement);
                                        await Task.Delay(400);
                                        ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].click();", accordionElement);
                                        await Task.Delay(400);
                                        var reportElement = wait.Until(ExpectedConditions.ElementIsVisible(By.XPath("//table[contains(@class, 'report')]")));
                                        string reportHtml = reportElement.GetAttribute("outerHTML");
                                        var parsedHtmlElements = await ParseHtmlForElementsOfInterest(reportHtml, isAnnualReport, companyName, companySymbol, companyId, dataNonStatic, statementName);
                                        if (parsedHtmlElements.Count == 0)
                                        {
                                            continue;
                                        }
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
            }
            finally
            {
                driverSemaphore.Release(); // Release the driver slot
            }
        }
        public static async Task<List<string>> ParseHtmlForElementsOfInterest(
    string htmlContent,
    bool isAnnualReport,
    string companyName,
    string companySymbol,
    int companyId,
    DataNonStatic dataNonStatic,
    string statementName)
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
                        continue;
                    }

                    // Step 1: Extract the fiscal year end date from the table headers
                    DateTime? fiscalYearEndDate = ExtractDateFromThTags(table); // Extract fiscal year end date from the table headers
                    if (!fiscalYearEndDate.HasValue)
                    {
                        Console.WriteLine($"[ERROR] Could not extract the fiscal year end date for {companyName} ({companySymbol}).");
                        continue;
                    }

                    // Step 2: Adjust fiscal year end date based on report type
                    DateTime adjustedFiscalYearEndDate = isAnnualReport
                       ? fiscalYearEndDate.Value // Do not adjust for annual reports
                       : Data.Data.AdjustToNearestQuarterEndDate(fiscalYearEndDate.Value); // Adjust for quarterly reports
                    Console.WriteLine($"[DEBUG] Adjusted fiscal year end date: {adjustedFiscalYearEndDate.ToShortDateString()} for {companyName} ({companySymbol})");

                    // Step 3: Determine Fiscal Year and Quarter
                    int fiscalYear;
                    int quarter;
                    DateTime fiscalYearEndDateFinal;

                    if (isAnnualReport)
                    {
                        // **For Annual Reports:** Set quarter to 0 and fiscal year based on fiscalYearEndDate
                        quarter = 0;
                        fiscalYear = adjustedFiscalYearEndDate.Year;
                        fiscalYearEndDateFinal = adjustedFiscalYearEndDate;
                        Console.WriteLine($"[DEBUG] Set Fiscal Year to {fiscalYear} and Quarter to {quarter} for Annual Report for CompanyID: {companyId}");
                    }
                    else
                    {
                        // **For Quarterly Reports:** Use DetermineFiscalYearAndQuarterAsync
                        (int determinedFiscalYear, int determinedQuarter, DateTime determinedFiscalYearEndDate) = await Data.Data.DetermineFiscalYearAndQuarterAsync(companyId, adjustedFiscalYearEndDate, dataNonStatic);
                        fiscalYear = determinedFiscalYear;
                        quarter = determinedQuarter;
                        fiscalYearEndDateFinal = determinedFiscalYearEndDate;
                        Console.WriteLine($"[DEBUG] Determined Fiscal Year: {fiscalYear}, Quarter: {quarter} for Quarterly Report for CompanyID: {companyId}");
                    }

                    // Step 4: Calculate Report Start and End Dates Based on Fiscal Year and Quarter
                    DateTime reportStartDate;
                    DateTime reportEndDate;

                    if (isAnnualReport)
                    {
                        // **For Annual Reports:**
                        reportEndDate = fiscalYearEndDateFinal;
                        reportStartDate = reportEndDate.AddYears(-1).AddDays(1);
                        Console.WriteLine($"[DEBUG] Annual Report's Period Start Date: {reportStartDate.ToShortDateString()}");
                        Console.WriteLine($"[DEBUG] Annual Report's Period End Date: {reportEndDate.ToShortDateString()}");
                    }
                    else
                    {
                        // **For Quarterly Reports:**
                        DateTime fiscalYearStartDate = fiscalYearEndDateFinal.AddYears(-1).AddDays(1).Date;
                        DateTime periodStart, periodEnd;

                        switch (quarter)
                        {
                            case 1:
                                periodStart = fiscalYearStartDate;
                                periodEnd = fiscalYearStartDate.AddMonths(3).AddDays(-1);
                                break;
                            case 2:
                                periodStart = fiscalYearStartDate.AddMonths(3);
                                periodEnd = fiscalYearStartDate.AddMonths(6).AddDays(-1);
                                break;
                            case 3:
                                periodStart = fiscalYearStartDate.AddMonths(6);
                                periodEnd = fiscalYearStartDate.AddMonths(9).AddDays(-1);
                                break;
                            case 4:
                                periodStart = fiscalYearStartDate.AddMonths(9);
                                periodEnd = fiscalYearEndDateFinal;
                                break;
                            default:
                                throw new ArgumentException("Invalid quarter value", nameof(quarter));
                        }

                        reportStartDate = periodStart;
                        reportEndDate = periodEnd;

                        Console.WriteLine($"[DEBUG] Quarterly Report's Period Start Date: {reportStartDate.ToShortDateString()}");
                        Console.WriteLine($"[DEBUG] Quarterly Report's Period End Date: {reportEndDate.ToShortDateString()}");
                    }

                    // Step 5: Initialize FinancialDataEntry with Report Dates
                    var parsedData = new FinancialDataEntry
                    {
                        CompanyID = companyId,
                        StartDate = reportStartDate,               // Actual Period Start Date
                        EndDate = reportEndDate,                   // Actual Period End Date
                        Quarter = quarter,
                        Year = fiscalYear,
                        IsHtmlParsed = true,  // Set to true for HTML parsing
                        IsXbrlParsed = false, // Set to false for HTML parsing
                        FinancialValues = new Dictionary<string, object>(),
                        FinancialValueTypes = new Dictionary<string, Type>(),
                        FiscalYearEndDate = fiscalYearEndDateFinal // Fiscal Year End Date
                    };

                    // Step 6: Extract Scaling Factors
                    var (sharesMultiplier, dollarMultiplier) = ExtractScalingFactor(htmlDocument);
                    Console.WriteLine($"[DEBUG] Extracted scaling factors: SharesMultiplier = {sharesMultiplier}, DollarMultiplier = {dollarMultiplier}");

                    // Step 7: Process the Data Rows
                    foreach (var row in rows)
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
                            var (cleanedValue, multiplier) = CleanNumber(rawValue, sharesMultiplier, dollarMultiplier);
                            if (double.TryParse(cleanedValue, out double value))
                            {
                                value *= multiplier;
                            }
                            else
                            {
                                continue;
                            }

                            string normalizedStatementType = NormalizeStatementName(statementName);
                            string elementName = ConstructHtmlElementName(quarter, fiscalYear, normalizedStatementType, label); // Construct the element name  
                            parsedData.FinancialValues[elementName] = value; // Add the element to the parsed data
                            parsedData.FinancialValueTypes[elementName] = typeof(double);
                            elements.Add(label); // Add the element label to the parsed elements list
                        }
                    }

                    Console.WriteLine($"[DEBUG] FinancialDataEntry populated with {parsedData.FinancialValues.Count} financial values.");

                    // Step 8: Set Year and Quarter Explicitly
                    if (isAnnualReport)
                    {
                        parsedData.Quarter = 0;
                        parsedData.Year = fiscalYear; // Set Year to the year of EndDate
                        Console.WriteLine($"[DEBUG] Set Quarter to 0 and Year to {parsedData.Year} for Annual Report for CompanyID: {companyId}");
                    }
                    else
                    {
                        // For quarterly reports, Year is already set via DetermineFiscalYearAndQuarterAsync
                        Console.WriteLine($"[DEBUG] Set Year to {parsedData.Year} for Quarterly Report for CompanyID: {companyId}");
                    }

                    // Step 9: Add the fully populated FinancialDataEntry to dataNonStatic
                    await dataNonStatic.AddParsedDataAsync(companyId, parsedData);
                    Console.WriteLine($"[INFO] Added FinancialDataEntry for CompanyID: {companyId}, FiscalYear: {fiscalYear}, Quarter: {quarter}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception occurred while parsing HTML for company {companyName}: {ex.Message}");
                Console.WriteLine($"[ERROR] Stack Trace: {ex.StackTrace}"); // Added for detailed debugging
            }

            return elements;
        }
        private static async Task<int> DetermineQuarter(int companyId, DateTime reportDate)
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                await connection.OpenAsync();
                // Fetch fiscalYearEndDate for the company from the database
                DateTime fiscalYearEndDate = Data.Data.GetFiscalYearEndForSpecificYearWithFallback(companyId, reportDate.Year, connection, null);
                // Call CalculateQuarterByFiscalDayMonth with the fetched fiscalYearEndDate
                return Data.Data.CalculateQuarterByFiscalDayMonth(reportDate, fiscalYearEndDate);
            }
        }
        private static string GetStatementType(string elementName)
        {
            if (elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
            {
                // For HTML elements, extract the statement type
                var parts = elementName.Split('_');
                if (parts.Length >= 3)
                {
                    // Normalize the extracted statement name
                    string rawStatementType = parts[2].Trim();
                    string normalizedName = NormalizeStatementName(rawStatementType);

                    if (normalizedStatementTypeMapping.TryGetValue(normalizedName, out string standardizedName))
                    {
                        return standardizedName;
                    }
                    else
                    {
                        return "Other";
                    }
                }
            }
            else
            {
                // For XBRL elements, use keywords to identify the statement type
                string upperElementName = elementName.ToUpperInvariant();

                if (upperElementName.Contains("CASHFLOW"))
                {
                    return "Cashflow_Statement";
                }
                else if (upperElementName.Contains("BALANCE"))
                {
                    return "Balance_Sheet";
                }
                else if (upperElementName.Contains("INCOME"))
                {
                    return "Income_Statement";
                }
                else if (upperElementName.Contains("OPERATIONS"))
                {
                    return "Statement_of_Operations";
                }
                else
                {
                    // Normalize for other statements
                    string normalizedName = NormalizeStatementName(upperElementName);

                    if (normalizedStatementTypeMapping.TryGetValue(normalizedName, out string standardizedName))
                    {
                        return standardizedName;
                    }
                    else
                    {
                        return "Other";
                    }
                }
            }
            return "Other";
        }
        private static string NormalizeStatementName(string rawName)
        {
            // Define common variations or unnecessary words to be normalized
            string[] wordsToRemove = new[]
            {
                "condensed", "(unaudited)", "consolidated", "interim",
                "statements of", "statement of", "statements", "statement", "the", "and"
            };

            // Remove parentheses and extra spaces
            string normalized = Regex.Replace(rawName, @"\s*\([^)]*\)", "").Trim(); // Remove text within parentheses
            normalized = Regex.Replace(normalized, @"\s+", " "); // Replace multiple spaces with a single space

            // Remove unnecessary words and phrases
            foreach (var word in wordsToRemove)
            {
                normalized = Regex.Replace(normalized, @"\b" + Regex.Escape(word) + @"\b", "", RegexOptions.IgnoreCase).Trim();
            }

            return normalized.ToLowerInvariant(); // Use lower case for consistent matching
        }
        private static string AdjustElementNameForQ4(string elementName, string standardizedStatementType = null)
        {
            if (elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
            {
                // Split the element name
                var parts = elementName.Split('_');
                if (parts.Length >= 4)
                {
                    // Normalize the statement type
                    string rawStatementType = parts[2].Trim();
                    string normalizedName = NormalizeStatementName(rawStatementType);

                    string standardizedStatementTypeFinal;
                    if (standardizedStatementType != null)
                    {
                        standardizedStatementTypeFinal = standardizedStatementType;
                    }
                    else if (normalizedStatementTypeMapping.TryGetValue(normalizedName, out string mappedType))
                    {
                        standardizedStatementTypeFinal = mappedType;
                    }
                    else
                    {
                        standardizedStatementTypeFinal = "Other";
                    }

                    // Replace parts[2] with standardizedStatementTypeFinal (replace spaces with underscores)
                    parts[2] = standardizedStatementTypeFinal.Replace(" ", "_");

                    // Replace the quarter part with "Q4Report"
                    parts[1] = "Q4Report";

                    // Reconstruct the adjusted element name
                    string adjustedElementName = string.Join("_", parts);
                    return adjustedElementName;
                }
                else
                {
                    // If element name does not have expected parts, just replace QxReport with Q4Report
                    string pattern = @"^HTML_Q\dReport_";
                    string replacement = "HTML_Q4Report_";
                    string adjustedElementName = Regex.Replace(elementName, pattern, replacement, RegexOptions.IgnoreCase);
                    return adjustedElementName;
                }
            }
            else
            {
                // For XBRL elements, return the element name as is
                return elementName;
            }
        }
        public static DateTime? ExtractDateFromThTags(HtmlNode table)
        {
            // Select all relevant <th> elements (adjust XPath as needed)
            var thElements = table.SelectNodes(".//th[@class='th']//div");

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
                }
            }

            if (mostRecentDate.HasValue)
            {
                return mostRecentDate;
            }

            Console.WriteLine($"[ERROR] No valid date could be extracted from <th> elements.");
            return null;
        }
        // Helper method to validate date formats using regex
        public static bool IsValidDateFormat(string text)
        {
            // Regex to match common date formats like "Dec 31, 2022", "July 2023", etc.
            string pattern = @"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s\d{1,2},?\s?\d{4}$|" +
                             @"^\d{4}-\d{2}-\d{2}$|" +
                             @"^\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}$";
            return Regex.IsMatch(text, pattern);
        }
        public static (string cleanedValue, double multiplier) CleanNumber(string rawValue, double sharesMultiplier, double dollarMultiplier)
        {
            string cleanedValue = rawValue.Replace(",", "").Replace("$", "").Replace("(", "-").Replace(")", "").Trim();
            bool isShares = rawValue.Contains("shares", StringComparison.OrdinalIgnoreCase);
            double multiplier = isShares ? sharesMultiplier : dollarMultiplier;
            return (cleanedValue, multiplier);
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
