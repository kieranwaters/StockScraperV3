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
        public static async Task ParseTraditionalXbrlContent(
        string xbrlContent,
        bool isAnnualReport,
        string companyName,
        string companySymbol,
        DataNonStatic dataNonStatic,
        int companyId)
    {
        var parsedEntries = new List<FinancialDataEntry>();
        try
        {
            // Clean and parse the XBRL content
            string cleanedContent = CleanXbrlContent(xbrlContent);
            XDocument xDocument = TryParseXDocument(cleanedContent);
            if (xDocument == null)
            {
                Console.WriteLine("[ERROR] Failed to parse XBRL document.");
                return;
            }

            // Define namespaces
            XNamespace xbrli = "http://www.xbrl.org/2003/instance";
            XNamespace ix = "http://www.xbrl.org/inlineXBRL/2013-02-12";

            // Extract contexts with non-null IDs
            var contexts = xDocument.Root?.Descendants()
                .Where(e => e.Name.LocalName == "context")
                .Select(e => new { Id = e.Attribute("id")?.Value, Element = e })
                .Where(x => !string.IsNullOrEmpty(x.Id))
                .ToDictionary(x => x.Id, x => x.Element);

            if (contexts == null || contexts.Count == 0)
            {
                Console.WriteLine("[DEBUG] No contexts found in Traditional XBRL.");
                return;
            }

            // Find all valid end dates from contexts
            var endDates = contexts.Values
                .Select(ctx => DateTime.TryParse(
                    ctx.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value,
                    out DateTime date) ? date : (DateTime?)null)
                .Where(date => date.HasValue)
                .ToList();

            DateTime? mostRecentEndDate = endDates.Any() ? endDates.Max() : null;
            if (!mostRecentEndDate.HasValue)
            {
                Console.WriteLine("[DEBUG] No valid end dates found in contexts.");
                return;
            }

            // Filter contexts to include only those with the most recent end date
            contexts = contexts
                .Where(kvp =>
                {
                    var ctx = kvp.Value;
                    var endDateStr = ctx.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value;
                    if (DateTime.TryParse(endDateStr, out DateTime endDate))
                    {
                        return endDate == mostRecentEndDate.Value;
                    }
                    return false;
                })
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (contexts.Count == 0)
            {
                Console.WriteLine("[DEBUG] No contexts found with the most recent end date.");
                return;
            }

            // Extract elements without filtering by elementsOfInterest
            var elements = xDocument.Descendants()
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

            if (elements == null || !elements.Any())
            {
                Console.WriteLine("[DEBUG] No financial elements found in Traditional XBRL.");
                return;
            }

            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                await connection.OpenAsync();

                var mainContext = contexts.Values.FirstOrDefault();
                if (mainContext == null)
                {
                    Console.WriteLine("[DEBUG] No main context found.");
                    return;
                }

                // Extract dates from the main context
                DateTime? contextStartDate = DateTime.TryParse(
                    mainContext.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value,
                    out DateTime startDateValue) ? startDateValue : (DateTime?)null;
                DateTime? contextInstantDate = DateTime.TryParse(
                    mainContext.Descendants().FirstOrDefault(e => e.Name.LocalName == "instant")?.Value,
                    out DateTime instantDateValue) ? instantDateValue : (DateTime?)null;

                // Determine dates for FinancialDataEntry with adjustment
                DateTime entryEndDate = mostRecentEndDate.Value;
                DateTime entryStartDate = AdjustStartDate(entryEndDate, contextStartDate, contextInstantDate, isAnnualReport);

                // Calculate the quarter
                int quarter = isAnnualReport
                    ? 0
                    : Data.Data.CalculateQuarterByFiscalDayMonth(companyId, entryEndDate, connection, null, leewayDays: 15);

                // Initialize the FinancialDataEntry
                int fiscalYear = CompanyFinancialData.GetFiscalYear(entryEndDate, quarter);

                // Get Standard Period Dates based on Fiscal Year and Quarter
                (DateTime standardStartDate, DateTime standardEndDate) = Data.Data.GetStandardPeriodDates(fiscalYear, quarter);

                // Initialize the FinancialDataEntry with Standard Dates
                var parsedData = new FinancialDataEntry
                {
                    CompanyID = companyId,
                    StartDate = entryStartDate,               // Parsed StartDate
                    EndDate = entryEndDate,                   // Parsed EndDate
                    Quarter = quarter,
                    IsHtmlParsed = false,
                    IsXbrlParsed = true,
                    FinancialValues = new Dictionary<string, object>(),
                    FinancialValueTypes = new Dictionary<string, Type>(),
                    StandardStartDate = standardStartDate,    // Set StandardStartDate
                    StandardEndDate = standardEndDate         // Set StandardEndDate
                };

                // Assign financial values
                foreach (var element in elements)
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

                // Add the fully populated FinancialDataEntry to parsedEntries and dataNonStatic
                parsedEntries.Add(parsedData);
                await dataNonStatic.AddParsedDataAsync(companyId, parsedData); // Updated to use the asynchronous method
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Exception in ParseTraditionalXbrlContent: {ex.Message}");
        }

        await Task.CompletedTask;
    }
    public static async Task ParseInlineXbrlContent(
    string htmlContent,
    bool isAnnualReport,
    string companyName,
    string companySymbol,
    DataNonStatic dataNonStatic,
    int companyId)
    {
        var parsedEntries = new List<FinancialDataEntry>(); // List to accumulate parsed entries
        try
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                await connection.OpenAsync();
                XDocument xDoc = XDocument.Parse(htmlContent);
                XNamespace ix = "http://www.xbrl.org/inlineXBRL/2013-02-12";
                XNamespace xbrli = "http://www.xbrl.org/2003/instance";

                var contexts = new Dictionary<string, XElement>();

                foreach (var contextNode in xDoc.Descendants(xbrli + "context")) // Extract contexts
                {
                    var id = contextNode.Attribute("id")?.Value;
                    if (!string.IsNullOrEmpty(id))
                    {
                        contexts[id] = contextNode; // Safe assignment
                    }
                }

                DateTime? mostRecentEndDate = contexts.Values // Find the most recent end date from all contexts
                    .Select(ctx => DateTime.TryParse(
                        ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                        out DateTime date) ? date : (DateTime?)null)
                    .Where(date => date.HasValue)
                    .Max();

                if (!mostRecentEndDate.HasValue)
                {
                    Console.WriteLine("[DEBUG] No valid end dates found in contexts.");
                    return;
                }

                var elements = xDoc.Descendants()
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

                if (elements.Count != 0)
                {
                    var mainContext = contexts.Values
                        .FirstOrDefault(ctx => DateTime.TryParse(
                            ctx.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                            out DateTime date) && date == mostRecentEndDate.Value);

                    if (mainContext == null)
                    {
                        Console.WriteLine("[DEBUG] Main context not found.");
                        return;
                    }

                    DateTime? contextStartDate = DateTime.TryParse(
                        mainContext.Descendants(xbrli + "startDate").FirstOrDefault()?.Value,
                        out DateTime startDateValue) ? startDateValue : (DateTime?)null;

                    DateTime? contextEndDate = DateTime.TryParse(
                        mainContext.Descendants(xbrli + "endDate").FirstOrDefault()?.Value,
                        out DateTime endDateValue) ? endDateValue : (DateTime?)null;

                    if (!contextEndDate.HasValue)
                    {
                        Console.WriteLine("[DEBUG] Invalid context end date.");
                        return;
                    }

                    DateTime entryEndDate = contextEndDate.Value;
                    DateTime entryStartDate = isAnnualReport
                        ? AdjustStartDate(entryEndDate, contextStartDate, null, isAnnualReport)
                        : entryEndDate.AddMonths(-3);

                    int quarter = isAnnualReport
                        ? 0
                        : Data.Data.CalculateQuarterByFiscalDayMonth(companyId, entryEndDate, connection, null, 15);

                    //var parsedData = new FinancialDataEntry
                    //{
                    //    CompanyID = companyId,
                    //    StartDate = entryStartDate,
                    //    EndDate = entryEndDate,
                    //    Quarter = quarter,
                    //    IsHtmlParsed = false,
                    //    IsXbrlParsed = true,
                    //    FinancialValues = new Dictionary<string, object>(),
                    //    FinancialValueTypes = new Dictionary<string, Type>()
                    //};
                    // Calculate Fiscal Year based on EndDate and Quarter
                    int fiscalYear = CompanyFinancialData.GetFiscalYear(entryEndDate, quarter);

                    // Get Standard Period Dates based on Fiscal Year and Quarter
                    (DateTime standardStartDate, DateTime standardEndDate) = Data.Data.GetStandardPeriodDates(fiscalYear, quarter);

                    // Initialize the FinancialDataEntry with Standard Dates
                    var parsedData = new FinancialDataEntry
                    {
                        CompanyID = companyId,
                        StartDate = entryStartDate,               // Parsed StartDate
                        EndDate = entryEndDate,                   // Parsed EndDate
                        Quarter = quarter,
                        IsHtmlParsed = false,
                        IsXbrlParsed = true,
                        FinancialValues = new Dictionary<string, object>(),
                        FinancialValueTypes = new Dictionary<string, Type>(),
                        StandardStartDate = standardStartDate,    // Set StandardStartDate
                        StandardEndDate = standardEndDate         // Set StandardEndDate
                    };


                    foreach (var element in elements)
                    {
                        if (string.IsNullOrEmpty(element.ContextRef) || !contexts.TryGetValue(element.ContextRef, out XElement context))
                        {
                            continue;
                        }

                        // Parse and assign financial value
                        if (decimal.TryParse(element.Value, out decimal parsedValue))
                        {
                            int decimalPlaces = int.TryParse(element.Decimals, out int dec) ? dec : 0;
                            parsedValue /= (decimal)Math.Pow(10, decimalPlaces); // Adjust for decimals

                            if (!string.IsNullOrEmpty(element.NormalisedName))
                            {
                                parsedData.FinancialValues[element.NormalisedName] = parsedValue;
                                parsedData.FinancialValueTypes[element.NormalisedName] = typeof(decimal);
                            }
                        }
                    }

                    // Add the fully populated FinancialDataEntry to parsedEntries and dataNonStatic
                    parsedEntries.Add(parsedData);
                    await dataNonStatic.AddParsedDataAsync(companyId, parsedData); // Updated to use the asynchronous method
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Exception in ParseInlineXbrlContent: {ex.Message}");
        }
        await Task.CompletedTask;
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
            catch (Exception ex)
            {
                content = CleanXbrlContent(content);
                try
                {
                    return XDocument.Parse(content);
                }
                catch (Exception retryEx)
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
                { // Use contextStartDate if duration matches expected duration
                    return contextStartDate.Value;
                }
                else
                {   // Adjust startDate based on expected duration
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
            { // If no contextStartDate, calculate based on expected duration
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
                catch (HttpRequestException ex) when (attempt < maxRetries)
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