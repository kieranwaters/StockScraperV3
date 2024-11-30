using Data;
using System.Data.SqlClient;
using System.Xml.Linq;
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