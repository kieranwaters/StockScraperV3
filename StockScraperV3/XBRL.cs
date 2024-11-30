using DataElements;
using HtmlAgilityPack;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;
using Nasdaq100FinancialScraper;
using System.Net;

namespace XBRL
{
    public static class XBRL
    {
        private static readonly HttpClient httpClient = new HttpClient();  // Reused instance

        static XBRL()
        {
            httpClient.DefaultRequestHeaders.Add("User-Agent", "KieranWaters/1.0 (kierandpwaters@gmail.com)");
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        }
        public static async Task<string?> GetXbrlUrl(string filingUrl)
        {
            string response = await httpClient.GetStringAsync(filingUrl);
            HtmlDocument doc = new HtmlDocument();
            doc.LoadHtml(response);

            var xbrlNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '_xbrl.xml') or contains(@href, '_htm.xml')]");
            if (xbrlNode != null)
            {
                return $"https://www.sec.gov{xbrlNode.GetAttributeValue("href", string.Empty)}";
            }

            var txtNode = doc.DocumentNode.SelectSingleNode("//a[contains(@href, '.txt')]");
            return txtNode != null ? $"https://www.sec.gov{txtNode.GetAttributeValue("href", string.Empty)}" : null;
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

        public static async Task ReparseXbrlReports(int companyId, int periodId, string companyName, string companySymbol, List<SqlCommand> batchedCommands)
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
                // Attempt to get the XBRL URL for each filing
                string xbrlUrl = await GetXbrlUrl(filing.url);
                if (!string.IsNullOrEmpty(xbrlUrl))
                {
                    try
                    {
                        // Reset global dates
                        Nasdaq100FinancialScraper.Program.globalStartDate = null;
                        Nasdaq100FinancialScraper.Program.globalEndDate = null;
                        Nasdaq100FinancialScraper.Program.globalInstantDate = null;

                        // Download and parse XBRL data for each filing
                        bool isAnnualReport = filing.description.Contains("10-K");// HERE
                        await DownloadAndParseXbrlData(xbrlUrl, isAnnualReport, companyName, companySymbol,
                            ParseTraditionalXbrlContent,
                            ParseInlineXbrlContent);
                    }
                    catch (Exception ex)
                    {
                        // Log error if something goes wrong during the reparsing process
                        Console.WriteLine($"[ERROR] Exception during XBRL reparsing for {companyName} ({companySymbol}): {ex.Message}");
                    }
                }
                else
                {
                    // Log if no XBRL URL is found for a filing
                    Console.WriteLine($"[ERROR] No XBRL URL found for {companyName} ({companySymbol})");
                }
            }).ToList();

            // Wait for all tasks to complete
            await Task.WhenAll(tasks);

            // Update the ParsedFullXBRL column for this company and period using batched commands
            await UpdateParsedFullXbrlColumn(companyId, periodId, batchedCommands);
        }
        private static async Task UpdateParsedFullXbrlColumn(int companyId, int periodId, List<SqlCommand> batchedCommands)
        {
            string query = @"
        UPDATE FinancialData
        SET ParsedFullXBRL = 'Yes'
        WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";

            var command = new SqlCommand(query);
            command.Parameters.AddWithValue("@CompanyID", companyId);
            command.Parameters.AddWithValue("@PeriodID", periodId);

            batchedCommands.Add(command);  // Add the command to the provided batchedCommands list
        }

        public static async Task ParseInlineXbrlContent(string htmlContent, bool isAnnualReport, string companyName, string companySymbol)
        {
            try
            {
                var htmlDocument = new HtmlDocument();
                htmlDocument.LoadHtml(htmlContent);

                var elementsOfInterest = FinancialElementLists.ElementsOfInterest;
                var contexts = new Dictionary<string, XElement>();
                var elements = htmlDocument.DocumentNode.Descendants()
                    .Where(n => n.Name == "ix:nonNumeric" || n.Name == "ix:nonFraction")
                    .Select(n => new FinancialElementLists.FinancialElement
                    {
                        Name = n.GetAttributeValue("name", string.Empty).Split(':').Last(),
                        ContextRef = n.GetAttributeValue("contextRef", string.Empty),
                        Value = n.InnerText.Trim()
                    })
                    .Where(e => elementsOfInterest.Contains(e.Name) && !string.IsNullOrEmpty(e.Value))
                    .ToList();

                foreach (var contextNode in htmlDocument.DocumentNode.Descendants("xbrli:context"))
                {
                    var id = contextNode.GetAttributeValue("id", string.Empty);
                    if (!string.IsNullOrEmpty(id) && !contexts.ContainsKey(id))
                    {
                        contexts[id] = XElement.Parse(contextNode.OuterHtml);
                    }
                }

                bool isXbrlParsed = false;
                if (elements.Any())
                {
                    foreach (var element in elements.Where(e => HTML.HTML.IsRelevantPeriod(contexts![e.ContextRef!], isAnnualReport)))
                    {
                        var context = contexts[element.ContextRef!];

                        DateTime? contextStartDate = DateTime.TryParse(context.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value, out DateTime startDateValue) ? startDateValue : (DateTime?)null;
                        DateTime? contextEndDate = DateTime.TryParse(context.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value, out DateTime endDateValue) ? endDateValue : (DateTime?)null;
                        DateTime? contextInstantDate = DateTime.TryParse(context.Descendants().FirstOrDefault(e => e.Name.LocalName == "instant")?.Value, out DateTime instantDateValue) ? instantDateValue : (DateTime?)null;

                        if (contextInstantDate.HasValue && (!contextStartDate.HasValue || !contextEndDate.HasValue))
                        {
                            Nasdaq100FinancialScraper.Program.globalInstantDate = contextInstantDate;
                            Nasdaq100FinancialScraper.Program.globalEndDate = Nasdaq100FinancialScraper.Program.globalInstantDate;

                            if (isAnnualReport)
                            {
                                Nasdaq100FinancialScraper.Program.globalStartDate = Nasdaq100FinancialScraper.Program.globalEndDate?.AddYears(-1);
                            }
                            else
                            {
                                Nasdaq100FinancialScraper.Program.globalStartDate = Nasdaq100FinancialScraper.Program.globalEndDate?.AddMonths(-3);
                            }
                        }
                        else if (contextStartDate.HasValue && contextEndDate.HasValue)
                        {
                            Nasdaq100FinancialScraper.Program.globalStartDate = contextStartDate;
                            Nasdaq100FinancialScraper.Program.globalEndDate = contextEndDate;
                        }
                        else { }

                        var dbColumn = element.Name;
                        Data.Data.SaveToDatabase(dbColumn, element.Value!, context, elementsOfInterest.ToArray(), elements, isAnnualReport, companyName, companySymbol, isHtmlParsed: false, isXbrlParsed: true, startDate: Nasdaq100FinancialScraper.Program.globalStartDate,endDate: Nasdaq100FinancialScraper.Program.globalEndDate);
                        isXbrlParsed = true;
                    }
                }

                if (!Nasdaq100FinancialScraper.Program.globalEndDate.HasValue)
                {
                    Nasdaq100FinancialScraper.Program.globalEndDate = DateTime.Now;  // Fallback to current date if end date is missing
                }
                else
                {
                    
                }
                if (isXbrlParsed)
                {
                    Data.Data.SaveToDatabase(string.Empty, string.Empty, null, elementsOfInterest.ToArray(), null, isAnnualReport, companyName, companySymbol, isXbrlParsed: true, startDate: Nasdaq100FinancialScraper.Program.globalStartDate,
        endDate: Nasdaq100FinancialScraper.Program.globalEndDate);
                }
            }
            catch (Exception ex)
            {
            }

            await Task.CompletedTask;
        }

        public static async Task ParseTraditionalXbrlContent(string xbrlContent, bool isAnnualReport, string companyName, string companySymbol)
        {
            try
            {
                string cleanedContent = CleanXbrlContent(xbrlContent);
                XDocument xDocument = TryParseXDocument(cleanedContent);
                if (xDocument == null)
                {
                    Console.WriteLine("[ERROR] Failed to parse XBRL document.");
                    return;
                }

                var elementsOfInterest = FinancialElementLists.ElementsOfInterest;
                var contexts = xDocument.Root?.Descendants()
                                            .Where(e => e.Name.LocalName == "context")
                                            .ToDictionary(e => e.Attribute("id")?.Value!, e => e);
                var elements = xDocument.Root?.Descendants()
                    .Where(e => elementsOfInterest.Contains(e.Name.LocalName))
                    .Select(e => new DataElements.FinancialElementLists.FinancialElement
                    {
                        Name = e.Name.LocalName,
                        ContextRef = e.Attribute("contextRef")?.Value,
                        Value = e.Value?.Trim()
                    })
                    .Where(e => e.ContextRef != null && !string.IsNullOrEmpty(e.Value))
                    .ToList();

                if (elements != null && elements.Any() && contexts != null && contexts.Any())
                {
                    foreach (var context in contexts.Values)
                    {
                        DateTime? contextStartDate = DateTime.TryParse(context.Descendants().FirstOrDefault(e => e.Name.LocalName == "startDate")?.Value, out DateTime startDateValue) ? startDateValue : (DateTime?)null;
                        DateTime? contextEndDate = DateTime.TryParse(context.Descendants().FirstOrDefault(e => e.Name.LocalName == "endDate")?.Value, out DateTime endDateValue) ? endDateValue : (DateTime?)null;
                        DateTime? contextInstantDate = DateTime.TryParse(context.Descendants().FirstOrDefault(e => e.Name.LocalName == "instant")?.Value, out DateTime instantDateValue) ? instantDateValue : (DateTime?)null;

                        if (contextInstantDate.HasValue && (!contextStartDate.HasValue || !contextEndDate.HasValue))
                        {
                            Nasdaq100FinancialScraper.Program.globalInstantDate = contextInstantDate;
                            Nasdaq100FinancialScraper.Program.globalEndDate = Nasdaq100FinancialScraper.Program.globalInstantDate;

                            if (isAnnualReport)
                            {
                                Nasdaq100FinancialScraper.Program.globalStartDate = Nasdaq100FinancialScraper.Program.globalEndDate?.AddYears(-1);
                            }
                            else
                            {
                                Nasdaq100FinancialScraper.Program.globalStartDate = Nasdaq100FinancialScraper.Program.globalEndDate?.AddMonths(-3);
                            }
                        }
                        else if (contextStartDate.HasValue && contextEndDate.HasValue)
                        {
                            Nasdaq100FinancialScraper.Program.globalStartDate = contextStartDate;
                            Nasdaq100FinancialScraper.Program.globalEndDate = contextEndDate;
                        }
                        else
                        {
                        }

                        foreach (var element in elements.Where(e => e.ContextRef == context.Attribute("id")?.Value))
                        {
                            var dbColumn = element.Name;
                            Data.Data.SaveToDatabase(dbColumn, element.Value!, context, elementsOfInterest.ToArray(), elements, isAnnualReport, companyName, companySymbol, isXbrlParsed: true, startDate: Nasdaq100FinancialScraper.Program.globalStartDate,
        endDate: Nasdaq100FinancialScraper.Program.globalEndDate);
                        }
                    }
                }

                if (!Nasdaq100FinancialScraper.Program.globalEndDate.HasValue)
                {
                    Nasdaq100FinancialScraper.Program.globalEndDate = DateTime.Now;  // Fallback to current date if end date is missing
                }
                else
                {
                }

            }
            catch (Exception ex)
            {
            }

            await Task.CompletedTask;
        }

        public static async Task DownloadAndParseXbrlData(string filingUrl, bool isAnnualReport, string companyName, string companySymbol, Func<string, bool, string, string, Task> parseTraditionalXbrlContent, Func<string, bool, string, string, Task> parseInlineXbrlContent)
        {
            Task<string> contentTask = null;
            if (filingUrl.EndsWith("_htm.xml") || filingUrl.EndsWith("_xbrl.htm") || filingUrl.EndsWith(".xml"))
            {
                contentTask = GetXbrlFromHtml(filingUrl); // Get XBRL from HTML or XML
            }
            else if (filingUrl.EndsWith(".htm"))
            {
                contentTask = GetHtmlContent(filingUrl); // Get HTML content for inline XBRL
            }
            else if (filingUrl.EndsWith(".txt"))
            {
                contentTask = GetEmbeddedXbrlContent(filingUrl); // Get embedded XBRL content
            }
            else
            {
                return; // Exit the method as no valid task was set
            }
            if (contentTask != null)
            {
                var content = await contentTask;
                if (!string.IsNullOrEmpty(content))
                {
                    if (filingUrl.EndsWith(".htm"))
                    {
                        await parseInlineXbrlContent(content, isAnnualReport, companyName, companySymbol);
                    }
                    else
                    {
                        await parseTraditionalXbrlContent(content, isAnnualReport, companyName, companySymbol);
                    }
                }
                else
                {
                }
            }
            else
            {
            }
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
        private static async Task<string> GetXbrlFromHtml(string url) => await httpClient.GetStringAsync(url);
        public static async Task<string> GetHtmlContent(string url) => await httpClient.GetStringAsync(url);
        private static async Task<string> GetEmbeddedXbrlContent(string filingUrl)
        {
            int maxRetries = 3;
            int delay = 1000; // Start with a 2-second delay

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    // Attempt to fetch the content
                    string txtContent = await httpClient.GetStringAsync(filingUrl);
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
