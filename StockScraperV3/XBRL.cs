using Data;
using System.Data.SqlClient;
using DataElements;
using HtmlAgilityPack;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;
using Nasdaq100FinancialScraper;
using System.Net;
using Data;
using System.Data;
using System.Transactions;
using static StockScraperV3.URL;
using OfficeOpenXml.Packaging.Ionic.Zlib;
using OpenQA.Selenium.BiDi.Modules.BrowsingContext;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Diagnostics;
using System.Net.NetworkInformation;
using System;

namespace XBRL
{
    public static class XBRL
    {
        private static readonly SemaphoreSlim requestSemaphore = new SemaphoreSlim(1, 1);
        public static async Task<(string? url, bool isIxbrl)> GetXbrlUrl(string filingUrl)
        {    // Timeout for the HttpClient request
            int timeoutInSeconds = 15;
            int maxRetries = 3;
            int delayBetweenRetries = 1000;
            int attempt = 0;
            await requestSemaphore.WaitAsync(); // Use semaphore to limit concurrent requests
            try
            {
                while (attempt < maxRetries)
                {
                    attempt++;
                    try
                    {
                        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutInSeconds)))
                        {
                            string response = await HttpClientProvider.Client.GetStringAsync(filingUrl);
                            HtmlDocument doc = new HtmlDocument();
                            doc.LoadHtml(response);
                            var ixbrlNode = doc.DocumentNode.SelectSingleNode("//*[contains(text(), 'iXBRL')]");
                            bool isIxbrl = ixbrlNode != null;
                            if (isIxbrl)
                            {
                                var xbrlNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '_xbrl.xml') or contains(@href, '_htm.xml')]");
                                if (xbrlNode != null)
                                {
                                    string xbrlUrl = $"https://www.sec.gov{xbrlNode.GetAttributeValue("href", string.Empty)}";
                                    return (xbrlUrl, true);
                                }
                                var txtNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '.txt')]");
                                if (txtNode != null)
                                {
                                    string txtUrl = $"https://www.sec.gov{txtNode.GetAttributeValue("href", string.Empty)}";
                                    return (txtUrl, true);
                                }
                            }
                            else
                            {
                                var xbrlNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '_xbrl.xml') or contains(@href, '_htm.xml')]");
                                if (xbrlNode != null)
                                {
                                    string xbrlUrl = $"https://www.sec.gov{xbrlNode.GetAttributeValue("href", string.Empty)}";
                                    return (xbrlUrl, false);
                                }
                                var txtNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '.txt')]");
                                if (txtNode != null)
                                {
                                    string txtUrl = $"https://www.sec.gov{txtNode.GetAttributeValue("href", string.Empty)}";
                                    return (txtUrl, false);
                                }
                            }
                        }
                    }
                    catch (TaskCanceledException ex) when (ex.CancellationToken == default)
                    {
                        Console.WriteLine($"[ERROR] Request timed out on attempt {attempt} for {filingUrl}. Retrying...");
                    }
                    catch (HttpRequestException ex)
                    {
                        Console.WriteLine($"[ERROR] HTTP request failed on attempt {attempt}: {ex.Message}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Unexpected error on attempt {attempt}: {ex.Message}");
                    }    // Wait before the next retry
                    if (attempt < maxRetries)
                    {
                        int randomizedDelay = delayBetweenRetries + new Random().Next(1000, 3000); // Adding a 1-3 seconds random delay
                        await Task.Delay(randomizedDelay);
                        delayBetweenRetries *= 2; // Exponential backoff
                    }
                }
                Console.WriteLine("[INFO] Failed to retrieve XBRL URL after maximum retry attempts.");
                return (null, false);
            }
            finally
            {     // Release the semaphore
                requestSemaphore.Release();
            }
        }
        public static async Task<List<string>> ParseInlineXbrlContent(
    string xbrlContent,
    bool isAnnualReport,
    string companyName,
    string companySymbol,
    DataNonStatic dataNonStatic,
    int companyId)
        {
            var elements = new List<string>(); // This will store parsed element labels
            try
            {
                // Step 1: Clean and parse the XBRL content
                string cleanedContent = CleanXbrlContent(xbrlContent);
                XDocument xDocument = TryParseXDocument(cleanedContent);
                if (xDocument == null)
                {
                    Console.WriteLine("[ERROR] Failed to parse Inline XBRL document.");
                    return elements;
                }

                // Step 2: Define namespaces
                XNamespace xbrli = "http://www.xbrl.org/2003/instance";
                XNamespace ix = "http://www.xbrl.org/inlineXBRL/2013-02-12";

                // Step 3: Extract contexts with non-null IDs
                var contexts = xDocument.Root?.Descendants()
                    .Where(e => e.Name.LocalName == "context")
                    .Select(e => new { Id = e.Attribute("id")?.Value, Element = e })
                    .Where(x => !string.IsNullOrEmpty(x.Id))
                    .ToDictionary(x => x.Id, x => x.Element);

                if (contexts == null || contexts.Count == 0)
                {
                    Console.WriteLine("[DEBUG] No contexts found in Inline XBRL.");
                    return elements;
                }
                foreach (var ctx in contexts.Values)
                {
                    var endDateElement = ctx.Descendants(xbrli + "endDate").FirstOrDefault();
                    string endDateStr = endDateElement != null ? endDateElement.Value : "No endDate";
                }

                // Extract financial elements linked to contexts
                var elementsList = xDocument.Descendants()
                    .Where(n => n.Name.Namespace != xbrli && n.Name.Namespace != ix)
                    .Select(n => new
                    {
                        RawName = n.Name.LocalName,
                        NormalisedName = string.IsNullOrEmpty(n.Name.LocalName)
                            ? null
                            : StockScraperV3.URL.NormaliseElementName(n.Name.LocalName),
                        ContextRef = n.Attribute("contextRef")?.Value,
                        Value = n.Value.Trim(),
                        Decimals = n.Attribute("decimals")?.Value ?? "0"
                    })
                    .Where(e => !string.IsNullOrEmpty(e.Value)
                                && !string.IsNullOrEmpty(e.ContextRef)
                                && contexts.ContainsKey(e.ContextRef))
                    .ToList();

                if (elementsList == null || !elementsList.Any())
                {
                    Console.WriteLine("[DEBUG] No financial elements found in Inline XBRL.");
                    return elements;
                }

                // Step 5: Open SQL connection
                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
                {
                    await connection.OpenAsync();

                    XElement mainContext = null;
                    DateTime reportStartDate = DateTime.MinValue;
                    DateTime reportEndDate = DateTime.MinValue;
                    DateTime fiscalYearEndDate = DateTime.MinValue; // Initialize to default
                    int fiscalYear = 0;
                    int quarter = 0;

                    if (isAnnualReport)
                    {
                        // **Assign mainContext for Annual Reports**
                        mainContext = contexts.Values
                            .Where(ctx => DateTime.TryParse(ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value, out _))
                            .OrderByDescending(ctx => DateTime.Parse(ctx.Descendants(xbrli + "endDate").FirstOrDefault().Value))
                            .FirstOrDefault();

                        if (mainContext == null)
                        {
                            Console.WriteLine($"[ERROR] No suitable context found for Annual Report for CompanyID: {companyId}.");
                            return elements;
                        }

                        // **Parse the Fiscal Year End Date from mainContext**
                        DateTime? reportFiscalYearEndDate = DateTime.TryParse(
                            mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                            out DateTime fyEndDate) ? fyEndDate : (DateTime?)null;

                        if (!reportFiscalYearEndDate.HasValue)
                        {
                            return elements;
                        }

                        // **Assign Values for Annual Reports**
                        reportEndDate = reportFiscalYearEndDate.Value;
                        reportStartDate = reportEndDate.AddYears(-1).AddDays(1);
                        fiscalYear = reportEndDate.Year;
                        fiscalYearEndDate = reportFiscalYearEndDate.Value; // **FIX: Assign fiscalYearEndDate**
                        quarter = 0; // Set Quarter to 0 for Annual Reports
                    }
                    else
                    {
                        // **Select the context with the latest endDate for Quarterly Reports**
                        mainContext = contexts.Values
                            .Where(ctx => DateTime.TryParse(ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value, out _))
                            .OrderByDescending(ctx => DateTime.Parse(ctx.Descendants(xbrli + "endDate").FirstOrDefault().Value))
                            .FirstOrDefault();

                        if (mainContext == null)
                        {
      
                            return elements;
                        }

                        // **Parse the Report End Date from mainContext**
                        DateTime? reportEndDateNullable = DateTime.TryParse(
                            mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                            out DateTime parsedReportEndDate) ? parsedReportEndDate : (DateTime?)null;

                        if (!reportEndDateNullable.HasValue)
                        {
                           ;
                            return elements;
                        }

                        reportEndDate = reportEndDateNullable.Value;

                        // **Determine Fiscal Year and Quarter Using the Updated Method**
                        (int determinedFiscalYear, int determinedQuarter, DateTime determinedFiscalYearEndDate) = await Data.Data.DetermineFiscalYearAndQuarterAsync(
                            companyId,
                            reportEndDate,
                            dataNonStatic);

                        fiscalYear = determinedFiscalYear;
                        quarter = determinedQuarter;
                        fiscalYearEndDate = determinedFiscalYearEndDate;

                        // **Parse the Report Start Date based on fiscalYearEndDate and quarter**
                        DateTime fiscalYearStartDate = fiscalYearEndDate.AddYears(-1).AddDays(1).Date;
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
                                periodEnd = fiscalYearEndDate;
                                break;
                            default:
                                throw new ArgumentException("Invalid quarter value", nameof(quarter));
                        }

                        reportStartDate = periodStart;
                        reportEndDate = periodEnd;

                    }

                    // **Initialize FinancialDataEntry with Actual Report Dates**
                    var parsedData = new FinancialDataEntry
                    {
                        CompanyID = companyId,
                        StartDate = reportStartDate,               // Actual Period Start Date
                        EndDate = reportEndDate,                   // Actual Period End Date
                        Quarter = quarter,
                        Year = fiscalYear,
                        IsHtmlParsed = false,                      // Set appropriately
                        IsXbrlParsed = true,                       // Set appropriately
                        FinancialValues = new Dictionary<string, object>(),
                        FinancialValueTypes = new Dictionary<string, Type>(),
                        StandardStartDate = reportStartDate,        // Optional: Can be the same as StartDate
                        StandardEndDate = reportEndDate,            // Optional: Can be the same as EndDate
                        FiscalYearEndDate = fiscalYearEndDate        // Fiscal Year End Date
                    };

                    // **Assign Financial Values**
                    foreach (var element in elementsList)
                    {
                        if (decimal.TryParse(element.Value, out decimal decimalValue))
                        {
                            if (!string.IsNullOrEmpty(element.NormalisedName))
                            {
                                parsedData.FinancialValues[element.NormalisedName] = decimalValue;
                                parsedData.FinancialValueTypes[element.NormalisedName] = typeof(decimal);
                            }
                        }
                    }
                    if (isAnnualReport)
                    {
                        parsedData.Quarter = 0;
                        parsedData.Year = fiscalYear; // Set Year to the year of EndDate
                        
                    }
                    else
                    {
                        //parsedData.Year = fiscalYearEndDate.Year; // Set Year to the year of FiscalYearEndDate
                        fiscalYear = fiscalYear;
           
                    }

                    // **Add the Fully Populated FinancialDataEntry to dataNonStatic**
                    await dataNonStatic.AddParsedDataAsync(companyId, parsedData);
                    
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in ParseInlineXbrlContent: {ex.Message}");
            }
            return elements;
        }
        public static async Task<List<string>> ParseTraditionalXbrlContent(
    string xbrlContent,
    bool isAnnualReport,
    string companyName,
    string companySymbol,
    DataNonStatic dataNonStatic,
    int companyId)
        {
            var elements = new List<string>(); // This will store parsed element labels
            try
            {
                // Step 1: Clean and parse the XBRL content
                string cleanedContent = CleanXbrlContent(xbrlContent);
                XDocument xDocument = TryParseXDocument(cleanedContent);
                if (xDocument == null)
                {
                    Console.WriteLine("[ERROR] Failed to parse Traditional XBRL document.");
                    return elements;
                }

                // Step 2: Define namespaces
                XNamespace xbrli = "http://www.xbrl.org/2003/instance";
                XNamespace ix = "http://www.xbrl.org/inlineXBRL/2013-02-12";

                // Step 3: Extract contexts with non-null IDs
                var contexts = xDocument.Root?.Descendants()
                    .Where(e => e.Name.LocalName == "context")
                    .Select(e => new { Id = e.Attribute("id")?.Value, Element = e })
                    .Where(x => !string.IsNullOrEmpty(x.Id))
                    .ToDictionary(x => x.Id, x => x.Element);

                if (contexts == null || contexts.Count == 0)
                {
                    Console.WriteLine("[DEBUG] No contexts found in Traditional XBRL.");
                    return elements;
                }

           
                foreach (var ctx in contexts.Values)
                {
                    var endDateElement = ctx.Descendants(xbrli + "endDate").FirstOrDefault();
                    string endDateStr = endDateElement != null ? endDateElement.Value : "No endDate";
                }

                // Step 4: Extract financial elements linked to contexts
                var elementsList = xDocument.Descendants()
                    .Where(n => n.Name.Namespace != xbrli && n.Name.Namespace != ix)
                    .Select(n => new
                    {
                        RawName = n.Name.LocalName,
                        NormalisedName = string.IsNullOrEmpty(n.Name.LocalName)
                            ? null
                            : StockScraperV3.URL.NormaliseElementName(n.Name.LocalName),
                        ContextRef = n.Attribute("contextRef")?.Value,
                        Value = n.Value.Trim(),
                        Decimals = n.Attribute("decimals")?.Value ?? "0"
                    })
                    .Where(e => !string.IsNullOrEmpty(e.Value)
                                && !string.IsNullOrEmpty(e.ContextRef)
                                && contexts.ContainsKey(e.ContextRef))
                    .ToList();

                if (elementsList == null || !elementsList.Any())
                {
   
                    return elements;
                }

                // Step 5: Open SQL connection
                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
                {
                    await connection.OpenAsync();

                    XElement mainContext = null;
                    DateTime reportStartDate = DateTime.MinValue;
                    DateTime reportEndDate = DateTime.MinValue;
                    DateTime fiscalYearEndDate = DateTime.MinValue; // Initialize to default
                    int fiscalYear = 0;
                    int quarter = 0;

                    if (isAnnualReport)
                    {
                        // **Assign mainContext for Annual Reports**
                        mainContext = contexts.Values
                            .Where(ctx => DateTime.TryParse(ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value, out _))
                            .OrderByDescending(ctx => DateTime.Parse(ctx.Descendants(xbrli + "endDate").FirstOrDefault().Value))
                            .FirstOrDefault();

                        if (mainContext == null)
                        {

                            return elements;
                        }

                        // **Parse the Fiscal Year End Date from mainContext**
                        DateTime? reportFiscalYearEndDate = DateTime.TryParse(
                            mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                            out DateTime fyEndDate) ? fyEndDate : (DateTime?)null;

                        if (!reportFiscalYearEndDate.HasValue)
                        {

                            return elements;
                        }

                        // **Assign Values for Annual Reports**
                        reportEndDate = reportFiscalYearEndDate.Value;
                        reportStartDate = reportEndDate.AddYears(-1).AddDays(1);
                        fiscalYear = reportEndDate.Year;
                        fiscalYearEndDate = reportFiscalYearEndDate.Value; // **Ensure fiscalYearEndDate is assigned**
                        quarter = 0; // Set Quarter to 0 for Annual Reports

                    }
                    else
                    {
                        // **Select the context with the latest endDate for Quarterly Reports**
                        mainContext = contexts.Values
                            .Where(ctx => DateTime.TryParse(ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value, out _))
                            .OrderByDescending(ctx => DateTime.Parse(ctx.Descendants(xbrli + "endDate").FirstOrDefault().Value))
                            .FirstOrDefault();

                        if (mainContext == null)
                        {
                            Console.WriteLine($"[ERROR] No suitable context found for Quarterly Report for CompanyID: {companyId}.");
                            return elements;
                        }

                        // **Parse the Report End Date from mainContext**
                        DateTime? reportEndDateNullable = DateTime.TryParse(
                            mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                            out DateTime parsedReportEndDate) ? parsedReportEndDate : (DateTime?)null;

                        if (!reportEndDateNullable.HasValue)
                        {
     
                            return elements;
                        }

                        reportEndDate = reportEndDateNullable.Value;

                        // **Determine Fiscal Year and Quarter Using the Updated Method**
                        (int determinedFiscalYear, int determinedQuarter, DateTime determinedFiscalYearEndDate) = await Data.Data.DetermineFiscalYearAndQuarterAsync(
                            companyId,
                            reportEndDate,
                            dataNonStatic);

                        fiscalYear = determinedFiscalYear;
                        quarter = determinedQuarter;
                        fiscalYearEndDate = determinedFiscalYearEndDate;

                        // **Parse the Report Start Date based on fiscalYearEndDate and quarter**
                        DateTime fiscalYearStartDate = fiscalYearEndDate.AddYears(-1).AddDays(1).Date;
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
                                periodEnd = fiscalYearEndDate;
                                break;
                            default:
                                throw new ArgumentException("Invalid quarter value", nameof(quarter));
                        }

                        reportStartDate = periodStart;
                        reportEndDate = periodEnd;

                    }

                    // **Initialize FinancialDataEntry with Actual Report Dates**
                    var parsedData = new FinancialDataEntry
                    {
                        CompanyID = companyId,
                        StartDate = reportStartDate,               // Actual Period Start Date
                        EndDate = reportEndDate,                   // Actual Period End Date
                        Quarter = quarter,
                        Year = fiscalYear,
                        IsHtmlParsed = false,                      // Set appropriately
                        IsXbrlParsed = true,                       // Set appropriately
                        FinancialValues = new Dictionary<string, object>(),
                        FinancialValueTypes = new Dictionary<string, Type>(),
                        StandardStartDate = reportStartDate,        // Optional: Can be the same as StartDate
                        StandardEndDate = reportEndDate,
                        FiscalYearEndDate = fiscalYearEndDate        // Fiscal Year End Date
                    };

                    // **Assign Financial Values**
                    foreach (var element in elementsList)
                    {
                        if (decimal.TryParse(element.Value, out decimal decimalValue))
                        {
                            if (!string.IsNullOrEmpty(element.NormalisedName))
                            {
                                parsedData.FinancialValues[element.NormalisedName] = decimalValue;
                                parsedData.FinancialValueTypes[element.NormalisedName] = typeof(decimal);
                            }
                        }
                    }

                    // **Set Year and Quarter Explicitly**
                    if (isAnnualReport)
                    {
                        parsedData.Quarter = 0;
                        parsedData.Year = fiscalYear; // Set Year to the year of EndDate
                        
                    }
                    else
                    {
                        // Ensure that Year is correctly set
                        parsedData.Year = fiscalYear;
                        
                    }

                    // **Add the Fully Populated FinancialDataEntry to dataNonStatic**
                    await dataNonStatic.AddParsedDataAsync(companyId, parsedData);
                    
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in ParseTraditionalXbrlContent: {ex.Message}");
            }
            return elements;
        }


        public static DateTime AdjustEndDate(DateTime endDate, bool isAnnualReport)
        {
            if (isAnnualReport)
            {
                return endDate;
            }
            else
            {        // For quarterly reports, adjust to the nearest quarter end date
                return Data.Data.AdjustToNearestQuarterEndDate(endDate);
            }
        }
        public static async Task DownloadAndParseXbrlData(string filingUrl, bool isAnnualReport, string companyName, string companySymbol, Func<string, bool, string, string, Task> parseTraditionalXbrlContent, Func<string, bool, string, string, Task> parseInlineXbrlContent)
        {
            Task<string> contentTask = null;
            bool isIxbrl = filingUrl.StartsWith("ixbrl:");
            if (isIxbrl)
            {
                filingUrl = filingUrl.Substring(6); // Remove "ixbrl:" prefix
            }
            if (filingUrl.EndsWith("_htm.xml") || filingUrl.EndsWith("_xbrl.htm") || filingUrl.EndsWith(".xml"))
            {
                contentTask = GetXbrlFromHtml(filingUrl); // For XBRL in XML or HTML format
            }
            else if (filingUrl.EndsWith(".htm"))
            {
                contentTask = GetHtmlContent(filingUrl); // Inline XBRL
            }
            else if (filingUrl.EndsWith(".txt"))
            {
                contentTask = GetEmbeddedXbrlContent(filingUrl); // Embedded XBRL in TXT format
            }
            else
            {
                Console.WriteLine($"[WARNING] Unsupported file format for URL: {filingUrl}");
                return;
            }
            if (contentTask != null)
            {
                var content = await contentTask;
                if (!string.IsNullOrEmpty(content))
                {
                    if (isIxbrl)
                    {   // Inline XBRL format
                        await parseInlineXbrlContent(content, isAnnualReport, companyName, companySymbol);
                    }
                    else
                    {    // Traditional XBRL format
                        await parseTraditionalXbrlContent(content, isAnnualReport, companyName, companySymbol);
                    }
                }
                else
                {
                    Console.WriteLine("[ERROR] Content retrieval returned empty for XBRL filing.");
                }
            }
        }
        static XDocument TryParseXDocument(string content)
        {
            try
            {
                return XDocument.Parse(content);
            }
            catch (Exception)
            {
                content = CleanXbrlContent(content);
                try
                {
                    return XDocument.Parse(content);
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }
        private static DateTime AdjustStartDate(DateTime entryEndDate, DateTime? contextStartDate, DateTime? contextInstantDate, bool isAnnualReport)
        {
            if (contextStartDate.HasValue)
            {
                TimeSpan duration = entryEndDate - contextStartDate.Value;
                double expectedDays = isAnnualReport ? 365 : 90;
                double allowedVariance = isAnnualReport ? 60 : 30;
                if (Math.Abs(duration.TotalDays - expectedDays) <= allowedVariance)
                {
                    return contextStartDate.Value;
                }
                else
                {
                    DateTime adjustedStartDate = isAnnualReport
                        ? entryEndDate.AddYears(-1)
                        : entryEndDate.AddMonths(-3);
                    return adjustedStartDate;
                }
            }
            else if (contextInstantDate.HasValue)
            {
                return contextInstantDate.Value;
            }
            else
            {
                DateTime adjustedStartDate = isAnnualReport
                    ? entryEndDate.AddYears(-1)
                    : entryEndDate.AddMonths(-3);
                return adjustedStartDate;
            }
        }
        private static bool IsRelevantContext(DateTime? startDate, DateTime? endDate, DateTime? instantDate, bool isAnnualReport)
        {
            if (instantDate.HasValue)
            { // Instant contexts (e.g., balance sheet items) are always relevant
                return true;
            }
            else if (startDate.HasValue && endDate.HasValue)
            {
                double days = (endDate.Value - startDate.Value).TotalDays;
                if (isAnnualReport)
                {// Accept durations close to 365 days (±30 days)
                    return Math.Abs(days - 365) <= 30;
                }
                else
                {        // Accept durations close to 90 days (±15 days)
                    return Math.Abs(days - 90) <= 15;
                }
            }
            return false;
        }
        public static string CleanXbrlContent(string xbrlContent)
        {
            xbrlContent = xbrlContent.TrimStart('\uFEFF');
            if (!xbrlContent.Contains("xmlns:xbrli"))
            {
                xbrlContent = xbrlContent.Replace("<xbrli:xbrl", "<xbrli:xbrl xmlns:xbrli=\"http://www.xbrl.org/2003/instance\"");
            }
            int xmlDeclarationIndex = xbrlContent.IndexOf("<?xml");
            if (xmlDeclarationIndex > 0)
            {
                xbrlContent = xbrlContent.Substring(xmlDeclarationIndex);
            }
            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"<!--(.+?)-->", match =>
            {
                string comment = match.Groups[1].Value;
                comment = comment.Replace("--", "-");
                if (comment.EndsWith("-"))
                {
                    comment = comment.TrimEnd('-');
                }
                return $"<!--{comment}-->";
            });
            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"<!---+", "<!--");
            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"-+>", "-->");
            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"</([a-zA-Z0-9]+)\s*>", "</$1>");
            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"<([a-zA-Z0-9]+)\s*>", "<$1>");
            xbrlContent = xbrlContent.Replace("</ ", "</");
            xbrlContent = xbrlContent.Replace("< /", "</");
            xbrlContent = xbrlContent.Replace("</xbrli:xbrl", "</xbrli:xbrl>");
            xbrlContent = xbrlContent.TrimStart();
            return xbrlContent.Trim();
        }
        private static async Task<string> GetXbrlFromHtml(string url) => await HttpClientProvider.Client.GetStringAsync(url);
        public static async Task<string> GetHtmlContent(string url) => await HttpClientProvider.Client.GetStringAsync(url);
        private static async Task<string> GetEmbeddedXbrlContent(string filingUrl)
        {
            int maxRetries = 3;
            int delay = 1000; // Start with a 2-second delay
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {   // Attempt to fetch the content
                    string txtContent = await HttpClientProvider.Client.GetStringAsync(filingUrl);
                    return ExtractEmbeddedXbrlFromTxt(txtContent);
                }
                catch (HttpRequestException ) when (attempt < maxRetries)
                {
                    await Task.Delay(delay);
                    delay *= 2; // Exponential backoff
                }
            }
            throw new HttpRequestException($"Failed to retrieve content from {filingUrl} after {maxRetries} attempts.");
        }
        private static string ExtractEmbeddedXbrlFromTxt(string txtContent)
        {
            var possibleXbrlTags = new[] { "<xbrli:xbrl", "<XBRL>", "<xbrl>", "<XBRLDocument>", "<xbrli:xbrl>" };
            foreach (var startTag in possibleXbrlTags)
            {
                var endTag = startTag.Replace("<", "</");
                int startIndex = txtContent.IndexOf(startTag, StringComparison.OrdinalIgnoreCase);
                int endIndex = txtContent.IndexOf(endTag, startIndex + startTag.Length, StringComparison.OrdinalIgnoreCase);
                if (startIndex != -1 && endIndex != -1)
                {
                    return txtContent.Substring(startIndex, endIndex - startIndex + endTag.Length);
                }
            }
            return txtContent;
        }
    }
}

////using Data;
////using System.Data.SqlClient;
////using DataElements;
////using HtmlAgilityPack;
////using System.Data.SqlClient;
////using System.Net.Http;
////using System.Threading.Tasks;
////using System.Xml.Linq;
////using Nasdaq100FinancialScraper;
////using System.Net;
////using Data;
////using System.Data;
////using System.Transactions;
////using static StockScraperV3.URL;
////using OfficeOpenXml.Packaging.Ionic.Zlib;
////using OpenQA.Selenium.BiDi.Modules.BrowsingContext;
////using static System.Runtime.InteropServices.JavaScript.JSType;
////using System.Diagnostics;
////using System.Net.NetworkInformation;
////using System;

////namespace XBRL
////{
////    public static class XBRL
////    {
////        private static readonly SemaphoreSlim requestSemaphore = new SemaphoreSlim(1, 1);
////        public static async Task<(string? url, bool isIxbrl)> GetXbrlUrl(string filingUrl)
////        {    // Timeout for the HttpClient request
////            int timeoutInSeconds = 15;
////            int maxRetries = 3;
////            int delayBetweenRetries = 1000;
////            int attempt = 0;
////            await requestSemaphore.WaitAsync(); // Use semaphore to limit concurrent requests
////            try
////            {
////                while (attempt < maxRetries)
////                {
////                    attempt++;
////                    try
////                    {
////                        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutInSeconds)))
////                        {
////                            string response = await HttpClientProvider.Client.GetStringAsync(filingUrl);
////                            HtmlDocument doc = new HtmlDocument();
////                            doc.LoadHtml(response);
////                            var ixbrlNode = doc.DocumentNode.SelectSingleNode("//*[contains(text(), 'iXBRL')]");
////                            bool isIxbrl = ixbrlNode != null;
////                            if (isIxbrl)
////                            {
////                                var xbrlNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '_xbrl.xml') or contains(@href, '_htm.xml')]");
////                                if (xbrlNode != null)
////                                {
////                                    string xbrlUrl = $"https://www.sec.gov{xbrlNode.GetAttributeValue("href", string.Empty)}";
////                                    return (xbrlUrl, true);
////                                }
////                                var txtNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '.txt')]");
////                                if (txtNode != null)
////                                {
////                                    string txtUrl = $"https://www.sec.gov{txtNode.GetAttributeValue("href", string.Empty)}";
////                                    return (txtUrl, true);
////                                }
////                            }
////                            else
////                            {
////                                var xbrlNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '_xbrl.xml') or contains(@href, '_htm.xml')]");
////                                if (xbrlNode != null)
////                                {
////                                    string xbrlUrl = $"https://www.sec.gov{xbrlNode.GetAttributeValue("href", string.Empty)}";
////                                    return (xbrlUrl, false);
////                                }
////                                var txtNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '.txt')]");
////                                if (txtNode != null)
////                                {
////                                    string txtUrl = $"https://www.sec.gov{txtNode.GetAttributeValue("href", string.Empty)}";
////                                    return (txtUrl, false);
////                                }
////                            }
////                        }
////                    }
////                    catch (TaskCanceledException ex) when (ex.CancellationToken == default)
////                    {
////                        Console.WriteLine($"[ERROR] Request timed out on attempt {attempt} for {filingUrl}. Retrying...");
////                    }
////                    catch (HttpRequestException ex)
////                    {
////                        Console.WriteLine($"[ERROR] HTTP request failed on attempt {attempt}: {ex.Message}");
////                    }
////                    catch (Exception ex)
////                    {
////                        Console.WriteLine($"[ERROR] Unexpected error on attempt {attempt}: {ex.Message}");
////                    }    // Wait before the next retry
////                    if (attempt < maxRetries)
////                    {
////                        int randomizedDelay = delayBetweenRetries + new Random().Next(1000, 3000); // Adding a 1-3 seconds random delay
////                        await Task.Delay(randomizedDelay);
////                        delayBetweenRetries *= 2; // Exponential backoff
////                    }
////                }
////                Console.WriteLine("[INFO] Failed to retrieve XBRL URL after maximum retry attempts.");
////                return (null, false);
////            }
////            finally
////            {     // Release the semaphore
////                requestSemaphore.Release();
////            }
////        }
////        public static async Task ParseTraditionalXbrlContent(
////   string xbrlContent,
////   bool isAnnualReport,
////   string companyName,
////   string companySymbol,
////   DataNonStatic dataNonStatic,
////   int companyId)
////        {
////            var parsedEntries = new List<FinancialDataEntry>();

////            try
////            {
////                // Clean and parse the XBRL content
////                string cleanedContent = CleanXbrlContent(xbrlContent);
////                XDocument xDocument = TryParseXDocument(cleanedContent);
////                if (xDocument == null)
////                {
////                    Console.WriteLine("[ERROR] Failed to parse XBRL document.");
////                    return;
////                }

////                // Define namespaces
////                XNamespace xbrli = "http://www.xbrl.org/2003/instance";
////                XNamespace ix = "http://www.xbrl.org/inlineXBRL/2013-02-12";

////                // Extract contexts with non-null IDs
////                var contexts = xDocument.Root?.Descendants()
////                    .Where(e => e.Name.LocalName == "context")
////                    .Select(e => new { Id = e.Attribute("id")?.Value, Element = e })
////                    .Where(x => !string.IsNullOrEmpty(x.Id))
////                    .ToDictionary(x => x.Id, x => x.Element);

////                if (contexts == null || contexts.Count == 0)
////                {
////                    Console.WriteLine("[DEBUG] No contexts found in Traditional XBRL.");
////                    return;
////                }

////                // Extract financial elements
////                var elements = xDocument.Descendants()
////                    .Where(n => n.Name.Namespace != xbrli && n.Name.Namespace != ix)
////                    .Select(n => new
////                    {
////                        RawName = n.Name.LocalName,
////                        NormalisedName = string.IsNullOrEmpty(n.Name.LocalName)
////                            ? null
////                            : StockScraperV3.URL.NormaliseElementName(n.Name.LocalName),
////                        ContextRef = n.Attribute("contextRef")?.Value,
////                        Value = n.Value.Trim(),
////                        Decimals = n.Attribute("decimals")?.Value ?? "0"
////                    })
////                    .Where(e => !string.IsNullOrEmpty(e.Value)
////                                && !string.IsNullOrEmpty(e.ContextRef)
////                                && contexts.ContainsKey(e.ContextRef))
////                    .ToList();

////                if (elements == null || !elements.Any())
////                {
////                    Console.WriteLine("[DEBUG] No financial elements found in Traditional XBRL.");
////                    return;
////                }

////                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
////                {
////                    await connection.OpenAsync();

////                    var mainContext = contexts.Values.FirstOrDefault();
////                    if (mainContext == null)
////                    {
////                        Console.WriteLine("[DEBUG] No main context found.");
////                        return;
////                    }

////                    // Extract dates from the main context
////                    DateTime? contextStartDate = DateTime.TryParse(
////                        mainContext.Descendants(xbrli + "startDate").FirstOrDefault()?.Value,
////                        out DateTime startDateValue) ? startDateValue : (DateTime?)null;
////                    DateTime? contextInstantDate = DateTime.TryParse(
////                        mainContext.Descendants(xbrli + "instant").FirstOrDefault()?.Value,
////                        out DateTime instantDateValue) ? instantDateValue : (DateTime?)null;

////                    DateTime fiscalYearEndDate;

////                    if (isAnnualReport)
////                    {
////                        // **For Annual Reports:** Use the end date directly from the report's context
////                        DateTime? reportFiscalYearEndDate = DateTime.TryParse(
////                            mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
////                            out DateTime fyEndDate) ? fyEndDate : (DateTime?)null;

////                        if (!reportFiscalYearEndDate.HasValue)
////                        {
////                            Console.WriteLine($"[ERROR] No fiscal year end date found in Annual Report for CompanyID: {companyId}.");
////                            return;
////                        }

////                        fiscalYearEndDate = reportFiscalYearEndDate.Value;
////                    }
////                    else
////                    {
////                        // **For Quarterly Reports:** Find the most recent end date from contexts
////                        var endDates = contexts.Values
////                            .Select(ctx => DateTime.TryParse(
////                                ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
////                                out DateTime date) ? date : (DateTime?)null)
////                            .Where(date => date.HasValue)
////                            .ToList();

////                        DateTime? mostRecentEndDate = endDates.Any() ? endDates.Max() : null;
////                        if (!mostRecentEndDate.HasValue)
////                        {
////                            Console.WriteLine("[DEBUG] No valid end dates found in contexts.");
////                            return;
////                        }

////                        fiscalYearEndDate = mostRecentEndDate.Value;
////                    }

////                    // **Adjust EndDate Based on Report Type**
////                    DateTime adjustedFiscalYearEndDate = isAnnualReport
////                        ? fiscalYearEndDate // Do not adjust for annual reports
////                        : Data.Data.AdjustToNearestQuarterEndDate(fiscalYearEndDate); // Adjust for quarterly reports

////                    Console.WriteLine($"[DEBUG] Adjusted Fiscal Year End Date: {adjustedFiscalYearEndDate.ToShortDateString()} for CompanyID: {companyId}");

////                    // **Adjust StartDate Based on the Adjusted Fiscal Year End Date**
////                    DateTime entryStartDate = AdjustStartDate(adjustedFiscalYearEndDate, contextStartDate, contextInstantDate, isAnnualReport);
////                    Console.WriteLine($"[DEBUG] Adjusted StartDate: {entryStartDate.ToShortDateString()} for CompanyID: {companyId}");

////                    // **Determine Quarter**
////                    int quarter = isAnnualReport
////                        ? 0 // 0 indicates an annual report
////                        : Data.Data.CalculateQuarterByFiscalDayMonth(adjustedFiscalYearEndDate, fiscalYearEndDate, leewayDays: 15);

////                    Console.WriteLine($"[DEBUG] Determined Quarter: {quarter} for CompanyID: {companyId}");

////                    // **Determine Fiscal Year Using GetFiscalYear Method**
////                    int fiscalYear = Data.Data.GetFiscalYear(
////                        adjustedFiscalYearEndDate,
////                        quarter,
////                        fiscalYearEndDate);

////                    Console.WriteLine($"[DEBUG] Determined Fiscal Year: {fiscalYear} for CompanyID: {companyId}");

////                    // **Get Standardized Period Dates Based on Fiscal Year and Quarter**
////                    (DateTime standardStartDate, DateTime standardEndDate) = Data.Data.GetStandardPeriodDates(
////                        fiscalYear,
////                        quarter,
////                        fiscalYearEndDate);

////                    Console.WriteLine($"[DEBUG] Standard Period Dates: Start - {standardStartDate.ToShortDateString()}, End - {standardEndDate.ToShortDateString()} for CompanyID: {companyId}");

////                    // **Initialize the FinancialDataEntry with Standard Dates**
////                    var parsedData = new FinancialDataEntry
////                    {
////                        CompanyID = companyId,
////                        StartDate = standardStartDate,             // Adjusted StartDate
////                        EndDate = standardEndDate,                 // Adjusted EndDate
////                        Quarter = quarter,
////                        IsHtmlParsed = false,
////                        IsXbrlParsed = true,
////                        FinancialValues = new Dictionary<string, object>(),
////                        FinancialValueTypes = new Dictionary<string, Type>(),
////                        StandardStartDate = standardStartDate,      // Set StandardStartDate
////                        StandardEndDate = standardEndDate,          // Set StandardEndDate
////                        FiscalYearEndDate = adjustedFiscalYearEndDate // Set FiscalYearEndDate
////                    };

////                    // **Assign Financial Values**
////                    foreach (var element in elements)
////                    {
////                        if (decimal.TryParse(element.Value, out decimal decimalValue))
////                        {
////                            if (!string.IsNullOrEmpty(element.NormalisedName))
////                            {
////                                parsedData.FinancialValues[element.NormalisedName] = decimalValue;
////                                parsedData.FinancialValueTypes[element.NormalisedName] = typeof(decimal);
////                            }
////                        }
////                    }

////                    // **Add the Fully Populated FinancialDataEntry to dataNonStatic**
////                    await dataNonStatic.AddParsedDataAsync(companyId, parsedData); // Updated to use the asynchronous method

////                    Console.WriteLine($"[INFO] Added FinancialDataEntry for CompanyID: {companyId}, FiscalYear: {fiscalYear}, Quarter: {quarter}");
////                }
////            }
////            catch (Exception ex)
////            {
////                Console.WriteLine($"[ERROR] Exception in ParseTraditionalXbrlContent: {ex.Message}");
////            }
////        }

////        public static async Task<List<string>> ParseInlineXbrlContent(
////    string xbrlContent,
////    bool isAnnualReport,
////    string companyName,
////    string companySymbol,
////    DataNonStatic dataNonStatic,
////    int companyId)
////        {
////            var elements = new List<string>(); // This will store parsed element labels

////            try
////            {
////                // Step 1: Clean and parse the XBRL content
////                string cleanedContent = CleanXbrlContent(xbrlContent);
////                XDocument xDocument = TryParseXDocument(cleanedContent);
////                if (xDocument == null)
////                {
////                    Console.WriteLine("[ERROR] Failed to parse Inline XBRL document.");
////                    return elements;
////                }

////                // Step 2: Define namespaces
////                XNamespace xbrli = "http://www.xbrl.org/2003/instance";
////                XNamespace ix = "http://www.xbrl.org/inlineXBRL/2013-02-12";

////                // Step 3: Extract contexts with non-null IDs
////                var contexts = xDocument.Root?.Descendants()
////                    .Where(e => e.Name.LocalName == "context")
////                    .Select(e => new { Id = e.Attribute("id")?.Value, Element = e })
////                    .Where(x => !string.IsNullOrEmpty(x.Id))
////                    .ToDictionary(x => x.Id, x => x.Element);

////                if (contexts == null || contexts.Count == 0)
////                {
////                    Console.WriteLine("[DEBUG] No contexts found in Inline XBRL.");
////                    return elements;
////                }

////                // Optional: Log all contexts and their endDates for debugging
////                Console.WriteLine("[DEBUG] Listing all contexts and their endDates:");
////                foreach (var ctx in contexts.Values)
////                {
////                    var endDateStr = ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value;
////                    Console.WriteLine($"[DEBUG] Context ID: {ctx.Attribute("id")?.Value}, endDate: {endDateStr}");
////                }

////                // Step 4: Extract financial elements linked to contexts
////                var elementsList = xDocument.Descendants()
////                    .Where(n => n.Name.Namespace != xbrli && n.Name.Namespace != ix)
////                    .Select(n => new
////                    {
////                        RawName = n.Name.LocalName,
////                        NormalisedName = string.IsNullOrEmpty(n.Name.LocalName)
////                            ? null
////                            : StockScraperV3.URL.NormaliseElementName(n.Name.LocalName),
////                        ContextRef = n.Attribute("contextRef")?.Value,
////                        Value = n.Value.Trim(),
////                        Decimals = n.Attribute("decimals")?.Value ?? "0"
////                    })
////                    .Where(e => !string.IsNullOrEmpty(e.Value)
////                                && !string.IsNullOrEmpty(e.ContextRef)
////                                && contexts.ContainsKey(e.ContextRef))
////                    .ToList();

////                if (elementsList == null || !elementsList.Any())
////                {
////                    Console.WriteLine("[DEBUG] No financial elements found in Inline XBRL.");
////                    return elements;
////                }

////                // Step 5: Open SQL connection
////                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
////                {
////                    await connection.OpenAsync();

////                    XElement mainContext = null;
////                    DateTime fiscalYearEndDate;

////                    if (isAnnualReport)
////                    {
////                        // **For Annual Reports:** Select the context matching the report's fiscal year end date
////                        // Assuming the annual report has the latest endDate
////                        var annualContexts = contexts.Values
////                            .Where(ctx => DateTime.TryParse(ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value, out _))
////                            .ToList();

////                        if (!annualContexts.Any())
////                        {
////                            Console.WriteLine($"[ERROR] No contexts with valid endDate found in Annual Report for CompanyID: {companyId}.");
////                            return elements;
////                        }

////                        // Select the context with the maximum endDate
////                        mainContext = annualContexts.OrderByDescending(ctx =>
////                        {
////                            DateTime.TryParse(ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value, out DateTime date);
////                            return date;
////                        }).FirstOrDefault();

////                        if (mainContext == null)
////                        {
////                            Console.WriteLine($"[ERROR] No suitable context found for Annual Report for CompanyID: {companyId}.");
////                            return elements;
////                        }

////                        // Extract fiscalYearEndDate from mainContext
////                        DateTime? reportFiscalYearEndDate = DateTime.TryParse(
////                            mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
////                            out DateTime fyEndDate) ? fyEndDate : (DateTime?)null;

////                        if (!reportFiscalYearEndDate.HasValue)
////                        {
////                            Console.WriteLine($"[ERROR] No fiscal year end date found in Annual Report for CompanyID: {companyId}.");
////                            return elements;
////                        }

////                        fiscalYearEndDate = reportFiscalYearEndDate.Value;

////                        Console.WriteLine($"[DEBUG] Annual Report's Fiscal Year End Date: {fiscalYearEndDate.ToShortDateString()}");
////                        Console.WriteLine($"[DEBUG] Selected mainContext ID: {mainContext.Attribute("id")?.Value} for Annual Report.");
////                    }
////                    else
////                    {
////                        // **For Quarterly Reports:** Find the most recent end date from contexts
////                        var endDates = contexts.Values
////                            .Select(ctx => DateTime.TryParse(
////                                ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
////                                out DateTime date) ? date : (DateTime?)null)
////                            .Where(date => date.HasValue)
////                            .ToList();

////                        DateTime? mostRecentEndDate = endDates.Any() ? endDates.Max() : null;
////                        if (!mostRecentEndDate.HasValue)
////                        {
////                            Console.WriteLine("[DEBUG] No valid end dates found in contexts.");
////                            return elements;
////                        }

////                        fiscalYearEndDate = mostRecentEndDate.Value;

////                        // Select the context with the most recent endDate
////                        mainContext = contexts.Values.FirstOrDefault(ctx =>
////                            DateTime.TryParse(
////                                ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
////                                out DateTime date) && date == fiscalYearEndDate);

////                        if (mainContext == null)
////                        {
////                            Console.WriteLine($"[ERROR] No context found with the most recent endDate: {fiscalYearEndDate.ToShortDateString()} for CompanyID: {companyId}.");
////                            return elements;
////                        }

////                        Console.WriteLine($"[DEBUG] Quarterly Report's Fiscal Year End Date: {fiscalYearEndDate.ToShortDateString()}");
////                        Console.WriteLine($"[DEBUG] Selected mainContext ID: {mainContext.Attribute("id")?.Value} for Quarterly Report.");
////                    }

////                    // Step 6: Adjust Fiscal Year End Date
////                    DateTime adjustedFiscalYearEndDate = isAnnualReport
////                        ? fiscalYearEndDate // Do not adjust for annual reports
////                        : Data.Data.AdjustToNearestQuarterEndDate(fiscalYearEndDate); // Adjust for quarterly reports

////                    Console.WriteLine($"[DEBUG] Adjusted Fiscal Year End Date: {adjustedFiscalYearEndDate.ToShortDateString()} for CompanyID: {companyId}");

////                    // Step 7: Extract dates from the main context
////                    DateTime? contextStartDate = DateTime.TryParse(
////                        mainContext.Descendants(xbrli + "startDate").FirstOrDefault()?.Value,
////                        out DateTime startDateValue) ? startDateValue : (DateTime?)null;
////                    DateTime? contextInstantDate = DateTime.TryParse(
////                        mainContext.Descendants(xbrli + "instant").FirstOrDefault()?.Value,
////                        out DateTime instantDateValue) ? instantDateValue : (DateTime?)null;

////                    // Step 8: Adjust StartDate
////                    DateTime entryStartDate = AdjustStartDate(adjustedFiscalYearEndDate, contextStartDate, contextInstantDate, isAnnualReport);
////                    Console.WriteLine($"[DEBUG] Adjusted StartDate: {entryStartDate.ToShortDateString()} for CompanyID: {companyId}");

////                    // Step 9: Determine Quarter
////                    int quarter = isAnnualReport
////                        ? 0 // 0 indicates an annual report
////                        : Data.Data.CalculateQuarterByFiscalDayMonth(adjustedFiscalYearEndDate, fiscalYearEndDate, leewayDays: 15);

////                    Console.WriteLine($"[DEBUG] Determined Quarter: {quarter} for CompanyID: {companyId}");

////                    // Step 10: Determine Fiscal Year
////                    int fiscalYear = Data.Data.GetFiscalYear(
////                        adjustedFiscalYearEndDate,
////                        quarter,
////                        fiscalYearEndDate);

////                    Console.WriteLine($"[DEBUG] Determined Fiscal Year: {fiscalYear} for CompanyID: {companyId}");

////                    // Step 11: Get Standardized Period Dates
////                    (DateTime standardStartDate, DateTime standardEndDate) = Data.Data.GetStandardPeriodDates(
////                        fiscalYear,
////                        quarter,
////                        fiscalYearEndDate);

////                    Console.WriteLine($"[DEBUG] Standard Period Dates: Start - {standardStartDate.ToShortDateString()}, End - {standardEndDate.ToShortDateString()} for CompanyID: {companyId}");

////                    // Step 12: Initialize FinancialDataEntry
////                    var parsedData = new FinancialDataEntry
////                    {
////                        CompanyID = companyId,
////                        StartDate = standardStartDate,             // Adjusted StartDate
////                        EndDate = standardEndDate,                 // Adjusted EndDate
////                        Quarter = quarter,
////                        IsHtmlParsed = false,                      // Set to false for XBRL parsing
////                        IsXbrlParsed = true,                       // Set to true for XBRL parsing
////                        FinancialValues = new Dictionary<string, object>(),
////                        FinancialValueTypes = new Dictionary<string, Type>(),
////                        StandardStartDate = standardStartDate,      // Set StandardStartDate
////                        StandardEndDate = standardEndDate,          // Set StandardEndDate
////                        FiscalYearEndDate = adjustedFiscalYearEndDate // Set FiscalYearEndDate
////                    };

////                    // Step 13: Assign Financial Values
////                    Console.WriteLine($"[DEBUG] Assigning financial values to FinancialDataEntry.");
////                    foreach (var element in elementsList)
////                    {
////                        if (decimal.TryParse(element.Value, out decimal decimalValue))
////                        {
////                            if (!string.IsNullOrEmpty(element.NormalisedName))
////                            {
////                                parsedData.FinancialValues[element.NormalisedName] = decimalValue;
////                                parsedData.FinancialValueTypes[element.NormalisedName] = typeof(decimal);
////                                Console.WriteLine($"[DEBUG] Assigned {element.NormalisedName} = {decimalValue}");
////                            }
////                        }
////                        else
////                        {
////                            Console.WriteLine($"[WARNING] Unable to parse value '{element.Value}' for element '{element.RawName}'.");
////                        }
////                    }

////                    // Step 14: Add FinancialDataEntry to dataNonStatic
////                    await dataNonStatic.AddParsedDataAsync(companyId, parsedData);
////                    Console.WriteLine($"[INFO] Added FinancialDataEntry for CompanyID: {companyId}, FiscalYear: {fiscalYear}, Quarter: {quarter}");
////                }
////            }
////            catch (Exception ex)
////            {
////                Console.WriteLine($"[ERROR] Exception in ParseInlineXbrlContent: {ex.Message}");
////                Console.WriteLine($"[ERROR] Stack Trace: {ex.StackTrace}"); // Added for detailed debugging
////            }

////            return elements;
////        }

////        public static DateTime AdjustEndDate(DateTime endDate, bool isAnnualReport)
////        {
////            if (isAnnualReport)
////            {
////                // For annual reports, endDate typically aligns with the fiscal year end date.
////                // If no further adjustment is needed, return as is.
////                // Alternatively, implement logic if specific alignment is required.
////                return endDate;
////            }
////            else
////            {
////                // For quarterly reports, adjust to the nearest quarter end date
////                return Data.Data.AdjustToNearestQuarterEndDate(endDate);
////            }
////        }
////        public static async Task DownloadAndParseXbrlData(string filingUrl, bool isAnnualReport, string companyName, string companySymbol, Func<string, bool, string, string, Task> parseTraditionalXbrlContent, Func<string, bool, string, string, Task> parseInlineXbrlContent)
////        {
////            Task<string> contentTask = null;
////            bool isIxbrl = filingUrl.StartsWith("ixbrl:");
////            if (isIxbrl)
////            {
////                filingUrl = filingUrl.Substring(6); // Remove "ixbrl:" prefix
////            }
////            if (filingUrl.EndsWith("_htm.xml") || filingUrl.EndsWith("_xbrl.htm") || filingUrl.EndsWith(".xml"))
////            {
////                contentTask = GetXbrlFromHtml(filingUrl); // For XBRL in XML or HTML format
////            }
////            else if (filingUrl.EndsWith(".htm"))
////            {
////                contentTask = GetHtmlContent(filingUrl); // Inline XBRL
////            }
////            else if (filingUrl.EndsWith(".txt"))
////            {
////                contentTask = GetEmbeddedXbrlContent(filingUrl); // Embedded XBRL in TXT format
////            }
////            else
////            {
////                Console.WriteLine($"[WARNING] Unsupported file format for URL: {filingUrl}");
////                return;
////            }
////            if (contentTask != null)
////            {
////                var content = await contentTask;
////                if (!string.IsNullOrEmpty(content))
////                {
////                    if (isIxbrl)
////                    {   // Inline XBRL format
////                        await parseInlineXbrlContent(content, isAnnualReport, companyName, companySymbol);
////                    }
////                    else
////                    {    // Traditional XBRL format
////                        await parseTraditionalXbrlContent(content, isAnnualReport, companyName, companySymbol);
////                    }
////                }
////                else
////                {
////                    Console.WriteLine("[ERROR] Content retrieval returned empty for XBRL filing.");
////                }
////            }
////        }
////        static XDocument TryParseXDocument(string content)
////        {
////            try
////            {
////                return XDocument.Parse(content);
////            }
////            catch (Exception ex)
////            {
////                content = CleanXbrlContent(content);
////                try
////                {
////                    return XDocument.Parse(content);
////                }
////                catch (Exception retryEx)
////                {

////                    return null;
////                }
////            }
////        }


////        private static DateTime AdjustStartDate(DateTime entryEndDate, DateTime? contextStartDate, DateTime? contextInstantDate, bool isAnnualReport)
////        {
////            if (contextStartDate.HasValue)
////            {
////                TimeSpan duration = entryEndDate - contextStartDate.Value;
////                double expectedDays = isAnnualReport ? 365 : 90;
////                double allowedVariance = isAnnualReport ? 60 : 30;
////                if (Math.Abs(duration.TotalDays - expectedDays) <= allowedVariance)
////                {
////                    return contextStartDate.Value;
////                }
////                else
////                {
////                    DateTime adjustedStartDate = isAnnualReport
////                        ? entryEndDate.AddYears(-1)
////                        : entryEndDate.AddMonths(-3);
////                    return adjustedStartDate;
////                }
////            }
////            else if (contextInstantDate.HasValue)
////            {
////                return contextInstantDate.Value;
////            }
////            else
////            {
////                DateTime adjustedStartDate = isAnnualReport
////                    ? entryEndDate.AddYears(-1)
////                    : entryEndDate.AddMonths(-3);
////                return adjustedStartDate;
////            }
////        }


////        private static bool IsRelevantContext(DateTime? startDate, DateTime? endDate, DateTime? instantDate, bool isAnnualReport)
////        {
////            if (instantDate.HasValue)
////            { // Instant contexts (e.g., balance sheet items) are always relevant
////                return true;
////            }
////            else if (startDate.HasValue && endDate.HasValue)
////            {
////                double days = (endDate.Value - startDate.Value).TotalDays;
////                if (isAnnualReport)
////                {// Accept durations close to 365 days (±30 days)
////                    return Math.Abs(days - 365) <= 30;
////                }
////                else
////                {        // Accept durations close to 90 days (±15 days)
////                    return Math.Abs(days - 90) <= 15;
////                }
////            }
////            return false;
////        }
////        public static string CleanXbrlContent(string xbrlContent)
////        {
////            xbrlContent = xbrlContent.TrimStart('\uFEFF');
////            if (!xbrlContent.Contains("xmlns:xbrli"))
////            {
////                xbrlContent = xbrlContent.Replace("<xbrli:xbrl", "<xbrli:xbrl xmlns:xbrli=\"http://www.xbrl.org/2003/instance\"");
////            }
////            int xmlDeclarationIndex = xbrlContent.IndexOf("<?xml");
////            if (xmlDeclarationIndex > 0)
////            {
////                xbrlContent = xbrlContent.Substring(xmlDeclarationIndex);
////            }
////            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"<!--(.+?)-->", match =>
////            {
////                string comment = match.Groups[1].Value;
////                comment = comment.Replace("--", "-");
////                if (comment.EndsWith("-"))
////                {
////                    comment = comment.TrimEnd('-');
////                }
////                return $"<!--{comment}-->";
////            });
////            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"<!---+", "<!--");
////            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"-+>", "-->");
////            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"</([a-zA-Z0-9]+)\s*>", "</$1>");
////            xbrlContent = System.Text.RegularExpressions.Regex.Replace(xbrlContent, @"<([a-zA-Z0-9]+)\s*>", "<$1>");
////            xbrlContent = xbrlContent.Replace("</ ", "</");
////            xbrlContent = xbrlContent.Replace("< /", "</");
////            xbrlContent = xbrlContent.Replace("</xbrli:xbrl", "</xbrli:xbrl>");
////            xbrlContent = xbrlContent.TrimStart();
////            return xbrlContent.Trim();
////        }
////        private static async Task<string> GetXbrlFromHtml(string url) => await HttpClientProvider.Client.GetStringAsync(url);
////        public static async Task<string> GetHtmlContent(string url) => await HttpClientProvider.Client.GetStringAsync(url);
////        private static async Task<string> GetEmbeddedXbrlContent(string filingUrl)
////        {
////            int maxRetries = 3;
////            int delay = 1000; // Start with a 2-second delay
////            for (int attempt = 1; attempt <= maxRetries; attempt++)
////            {
////                try
////                {   // Attempt to fetch the content
////                    string txtContent = await HttpClientProvider.Client.GetStringAsync(filingUrl);
////                    return ExtractEmbeddedXbrlFromTxt(txtContent);
////                }
////                catch (HttpRequestException ex) when (attempt < maxRetries)
////                {
////                    await Task.Delay(delay);
////                    delay *= 2; // Exponential backoff
////                }
////            }
////            throw new HttpRequestException($"Failed to retrieve content from {filingUrl} after {maxRetries} attempts.");
////        }
////        private static string ExtractEmbeddedXbrlFromTxt(string txtContent)
////        {
////            var possibleXbrlTags = new[] { "<xbrli:xbrl", "<XBRL>", "<xbrl>", "<XBRLDocument>", "<xbrli:xbrl>" };
////            foreach (var startTag in possibleXbrlTags)
////            {
////                var endTag = startTag.Replace("<", "</");
////                int startIndex = txtContent.IndexOf(startTag, StringComparison.OrdinalIgnoreCase);
////                int endIndex = txtContent.IndexOf(endTag, startIndex + startTag.Length, StringComparison.OrdinalIgnoreCase);
////                if (startIndex != -1 && endIndex != -1)
////                {
////                    return txtContent.Substring(startIndex, endIndex - startIndex + endTag.Length);
////                }
////            }
////            return txtContent;
////        }
////    }
////}