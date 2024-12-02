using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Data;
using System.Text.RegularExpressions;

namespace Data
{
    public class CompanyFinancialData
    {
        public int CompanyID { get; }
        public ConcurrentDictionary<string, FinancialDataEntry> FinancialEntries { get; }
        public CompanyFinancialData(int companyId)
        {
            CompanyID = companyId;
            FinancialEntries = new ConcurrentDictionary<string, FinancialDataEntry>(StringComparer.OrdinalIgnoreCase);
        }
        public DateTime? GetMostRecentFiscalYearEndDate()
        {
            var annualReports = FinancialEntries.Values
                .Where(entry => entry.Quarter == 0)
                .OrderByDescending(entry => entry.EndDate)
                .ToList();
            if (annualReports.Any())
            {
                return annualReports.First().EndDate;
            }
            else
            {
                return null; // No annual reports found in cache
            }
        }
        public void AddOrUpdateEntry(FinancialDataEntry entry)
        {     // Define a leeway period (e.g., +/- 5 days)
            TimeSpan leeway = TimeSpan.FromDays(5);
            // Try to find an existing key within the leeway period
            var existingEntryKey = FinancialEntries.Keys.FirstOrDefault(key =>
            {
                var parts = key.Split('_');
                if (parts.Length != 3) return false;
                if (int.TryParse(parts[1], out int startDateInt) && int.TryParse(parts[2], out int endDateInt))
                {
                    DateTime existingStartDate = DateTime.ParseExact(startDateInt.ToString(), "yyyyMMdd", null);
                    DateTime existingEndDate = DateTime.ParseExact(endDateInt.ToString(), "yyyyMMdd", null);
                    return Math.Abs((existingStartDate - entry.StartDate).TotalDays) <= leeway.TotalDays &&
                           Math.Abs((existingEndDate - entry.EndDate).TotalDays) <= leeway.TotalDays;
                }
                return false;
            });
            if (existingEntryKey != null)
            {         // Merge with the existing entry
                FinancialEntries[existingEntryKey].MergeEntries(entry);
            }
            else
            {        // Add as a new entry
                string key = GenerateKey(entry);
                FinancialEntries[key] = entry;
            }
        }

        public static int GetFiscalYear(DateTime endDate, int quarter, DateTime? fiscalYearEndDate = null)
        {
            if (fiscalYearEndDate.HasValue)
            {
                DateTime fiscalYearStart = fiscalYearEndDate.Value.AddYears(-1).AddDays(1);
                if (endDate >= fiscalYearStart && endDate <= fiscalYearEndDate.Value)
                {
                    return fiscalYearEndDate.Value.Year;
                }
                else if (endDate > fiscalYearEndDate.Value)
                {
                    return fiscalYearEndDate.Value.AddYears(1).Year;
                }
                else
                {
                    return fiscalYearEndDate.Value.AddYears(-1).Year;
                }
            }
            else
            {    // Fallback to existing logic if fiscalYearEndDate is not available
                switch (quarter)
                {
                    case 1:
                        return endDate.AddMonths(-9).Year;
                    case 2:
                        return endDate.AddMonths(-6).Year;
                    case 3:
                        return endDate.AddMonths(-3).Year;
                    case 4:
                        return endDate.Year;
                    case 0:
                        return endDate.Year;
                    default:
                        throw new ArgumentException("Invalid quarter value", nameof(quarter));
                }
            }
        }
        private string GenerateKey(FinancialDataEntry entry)
        {    // Format the dates to a consistent string format (e.g., yyyyMMdd)
            string startDateStr = entry.StartDate.ToString("yyyyMMdd");
            string endDateStr = entry.EndDate.ToString("yyyyMMdd");
            string key = $"{entry.CompanyID}_{startDateStr}_{endDateStr}";
            Console.WriteLine($"[DEBUG] Generated Key: {key} for CompanyID: {entry.CompanyID}, StartDate: {entry.StartDate.ToShortDateString()}, EndDate: {entry.EndDate.ToShortDateString()}");
            return key;
        }
        public List<FinancialDataEntry> GetCompletedEntries()// Method to retrieve all completed entries
        {
            return FinancialEntries.Values.Where(entry => entry.IsEntryComplete()).ToList();
        }
    }
    public class FinancialDataEntry
    {
        public int CompanyID { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public int Quarter { get; set; }
        public bool IsHtmlParsed { get; set; }
        public bool IsXbrlParsed { get; set; }
        public DateTime? FiscalYearEndDate { get; set; }
        public Dictionary<string, object> FinancialValues { get; set; }
        public Dictionary<string, Type> FinancialValueTypes { get; set; }
        public FinancialDataEntry() // Constructor to initialize dictionaries
        {
            FinancialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            FinancialValueTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
        }
        public DateTime StandardStartDate { get; set; }
        public DateTime StandardEndDate { get; set; }
    }
    public static class DataExtensions
    {
        public static bool IsEntryComplete(this FinancialDataEntry entry)
        {
            return entry.IsXbrlParsed && entry.IsHtmlParsed;
        }
    }
    public class DataNonStatic
    {
        public ConcurrentDictionary<int, CompanyFinancialData> companyFinancialDataCache = new ConcurrentDictionary<int, CompanyFinancialData>();
        private const string connectionString = "Server=DESKTOP-SI08RN8\\SQLEXPRESS;Database=StockDataScraperDatabase;Integrated Security=True;";
        public async Task<CompanyFinancialData> GetOrLoadCompanyFinancialDataAsync(int companyId)
        { // Attempt to get the company's data from the cache
            if (companyFinancialDataCache.TryGetValue(companyId, out var companyData))
            {
                return companyData;
            }
            companyData = new CompanyFinancialData(companyId);// If not present, load from the database
            var entries = await LoadFinancialDataFromDatabaseAsync(companyId);
            foreach (var entry in entries)
            {
                companyData.AddOrUpdateEntry(entry);
            }
            companyFinancialDataCache[companyId] = companyData;// Add to the cache
            return companyData;
        }
        public async Task SaveEntriesToDatabaseAsync(int companyId, List<FinancialDataEntry> entries)
{
    if (entries == null || !entries.Any()) return;
    try
    {
        var companyData = await GetOrLoadCompanyFinancialDataAsync(companyId);
        foreach (var entry in entries)
        {
            companyData.AddOrUpdateEntry(entry);
        }
        var mergedEntries = companyData.FinancialEntries.Values
            .Where(e => e.IsEntryComplete())
            .ToList();
        if (!mergedEntries.Any())
        {
            return;
        }
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            using (SqlTransaction transaction = connection.BeginTransaction())
            {
                try
                {    // Define leeway (e.g., 5 days)
                    TimeSpan leeway = TimeSpan.FromDays(15);
                    await Data.SaveCompleteEntryToDatabase(mergedEntries, connection, transaction);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Transaction failed while saving entries: {ex.Message}");
                    try
                    {
                        transaction.Rollback();
                        Console.WriteLine("[INFO] Transaction rolled back.");
                    }
                    catch (Exception rollbackEx)
                    {
                        Console.WriteLine($"[ERROR] Transaction rollback failed: {rollbackEx.Message}");
                    }
                    throw;
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Failed to save entries to database: {ex.Message}");
    }
}
        private async Task<List<FinancialDataEntry>> LoadFinancialDataFromDatabaseAsync(int companyId)
        {
            var entries = new List<FinancialDataEntry>();
            string query = @"
SELECT CompanyID, StartDate, EndDate, Quarter, IsHtmlParsed, IsXbrlParsed, FinancialDataJson
FROM FinancialData
WHERE CompanyID = @CompanyID";
            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                await connection.OpenAsync();
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var entry = new FinancialDataEntry
                        {
                            CompanyID = reader.GetInt32(0),
                            StartDate = reader.GetDateTime(1),
                            EndDate = reader.GetDateTime(2),
                            Quarter = reader.GetInt32(3),
                            IsHtmlParsed = reader.GetBoolean(4),
                            IsXbrlParsed = reader.GetBoolean(5),
                            FinancialValues = Data.ParseFinancialDataJson(reader.GetString(6)),
                            FiscalYearEndDate = null // Initialized as null
                        };             
                        if (entry.Quarter == 0)// Set FiscalYearEndDate for annual reports
                        {
                            entry.FiscalYearEndDate = entry.EndDate;
                        }
                        entries.Add(entry);
                    }
                }
            }
            return entries;
        }
        public async Task<List<FinancialDataEntry>> GetCompletedEntriesAsync(int companyId)
        {
            var companyData = await GetOrLoadCompanyFinancialDataAsync(companyId);
            return companyData.GetCompletedEntries();
        }
        public async Task AddParsedDataAsync(int companyId, FinancialDataEntry parsedData)
        {
            var companyData = await GetOrLoadCompanyFinancialDataAsync(companyId);
            companyData.AddOrUpdateEntry(parsedData);// Optionally, save to the database immediately or batch the updates
            await SaveEntriesToDatabaseAsync(companyId, new List<FinancialDataEntry> { parsedData });
        }
        private ConcurrentDictionary<string, FinancialDataEntry> financialDataCache = new ConcurrentDictionary<string, FinancialDataEntry>();
    }
    public static class Data
    {
        private static void ProcessAllFinancialElements(SqlConnection connection, SqlTransaction transaction, int companyId, int year, Dictionary<string, object> q4Values, List<string> elementNames)
        {
            foreach (var elementName in elementNames)
            {   // Adjust the element name for annual value retrieval
                string annualElementName = AdjustElementNameForAnnual(elementName);
                string q1ElementName = AdjustElementNameForQuarter(elementName, 1);// Adjust the element names for each quarter
                string q2ElementName = AdjustElementNameForQuarter(elementName, 2);
                string q3ElementName = AdjustElementNameForQuarter(elementName, 3);
                // Retrieve financial values using the adjusted element names
                decimal? annualValue = GetFinancialValue(connection, transaction, companyId, year, 0, annualElementName);
                decimal? q1Value = GetNullableFinancialValue(connection, transaction, companyId, year, 1, q1ElementName);
                decimal? q2Value = GetNullableFinancialValue(connection, transaction, companyId, year, 2, q2ElementName);
                decimal? q3Value = GetNullableFinancialValue(connection, transaction, companyId, year, 3, q3ElementName);
                decimal? q4Value;
                if (annualValue.HasValue)
                {
                    bool isHtmlElement = elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase);
                    string statementType = GetStatementType(elementName);
                    bool isBalanceSheet = statementType.Equals("Balance Sheet", StringComparison.OrdinalIgnoreCase);
                    if (isBalanceSheet && !statementType.Equals("Other", StringComparison.OrdinalIgnoreCase))
                    {
                        if (isHtmlElement)
                        {   // For HTML Balance Sheet items not in "Other" category, Q4 = Annual value
                            q4Value = annualValue.Value;
                        }
                        else
                        {   // For XBRL Balance Sheet items not in "Other" category, attempt to get Q4 directly
                            q4Value = GetFinancialValue(connection, transaction, companyId, year, 4, elementName);
                            if (!q4Value.HasValue)
                            {
                                continue; // Or handle as needed
                            }
                        }
                    }
                    else
                    {   // For Income Statement, Cashflow, and "Other" items, Q4 = Annual - (Q1 + Q2 + Q3)
                        q4Value = annualValue.Value - (q1Value.GetValueOrDefault() + q2Value.GetValueOrDefault() + q3Value.GetValueOrDefault());
                    }
                    string adjustedElementName = AdjustElementNameForQ4(elementName); // Adjust the element name for Q4                    
                    q4Values[adjustedElementName] = q4Value;// Assign the value with the adjusted key
                }
            }
        }
        private static string GetStatementType(string elementName)
        {
            if (elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
            {   // For HTML elements, extract the statement type
                var parts = elementName.Split('_');
                if (parts.Length >= 3)
                {   // Normalize the extracted statement name
                    string rawStatementType = parts[2].Trim();
                    return NormalizeStatementName(rawStatementType);
                }
            }
            else
            {  // For XBRL elements, use keywords to identify the statement type
                string upperElementName = elementName.ToUpperInvariant();
                if (upperElementName.Contains("CASHFLOW"))
                {
                    return "Cashflow";
                }
                else if (upperElementName.Contains("BALANCE"))
                {
                    return "Balance Sheet";
                }
                else if (upperElementName.Contains("INCOME"))
                {
                    return "Income Statement";
                }
                else if (upperElementName.Contains("OPERATIONS"))
                {
                    return "Statement of Operations";
                }
                else
                { // Normalize for other statements
                    return NormalizeStatementName(upperElementName);
                }
            }
            return "Other";
        }
        private static string NormalizeStatementName(string rawName)
        {   // Define common variations or unnecessary words to be normalized
            string[] wordsToRemove = new[]
            {
            "condensed", "(unaudited)", "consolidated", "interim", "  statements of", "statement of", "statements", "statement", "the", "and"
        };       // Remove parentheses and extra spaces
            string normalized = Regex.Replace(rawName, @"\s*\([^)]*\)", "").Trim(); // Remove text within parentheses
            normalized = Regex.Replace(normalized, @"\s+", " "); // Replace multiple spaces with a single space           
            foreach (var word in wordsToRemove) // Remove unnecessary words and phrases
            {
                normalized = Regex.Replace(normalized, @"\b" + Regex.Escape(word) + @"\b", "", RegexOptions.IgnoreCase).Trim();
            }
            return normalized.ToLowerInvariant(); // Use lower case for consistent matching
        }
        private static string AdjustElementNameForAnnual(string elementName)
        {
            if (elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
            { // Replace the quarterly prefix with the annual prefix
                string pattern = @"^HTML_Q\dReport_";
                string replacement = "HTML_AnnualReport_";
                return Regex.Replace(elementName, pattern, replacement, RegexOptions.IgnoreCase);
            }
            else
            { // For XBRL elements, the element names are the same
                return elementName;
            }
        }
        private static string AdjustElementNameForQuarter(string elementName, int quarter)
        {
            if (elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
            {   // Replace the quarterly or annual prefix with the appropriate quarter
                string pattern = @"^HTML_(Q\dReport|AnnualReport)_";
                string replacement = $"HTML_Q{quarter}Report_";
                return Regex.Replace(elementName, pattern, replacement, RegexOptions.IgnoreCase);
            }
            else
            {// For XBRL elements, the element names are the same
                return elementName;
            }
        }
        private static string AdjustElementNameForQ4(string elementName)
        {
            if (elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase))
            {    // Replace the quarter in the element name with "Q4"
                string pattern = @"^HTML_(Q\dReport|AnnualReport)_";
                string replacement = "HTML_Q4Report_";
                return Regex.Replace(elementName, pattern, replacement, RegexOptions.IgnoreCase);
            }
            else
            {  // For XBRL elements, return the element name as is
                return elementName;
            }
        }
        private static decimal? GetFinancialValue(SqlConnection connection, SqlTransaction transaction, int companyId, int year, int quarter, string columnName)
        {
            string query = @"
        SELECT FinancialDataJson
        FROM FinancialData
        WHERE CompanyID = @CompanyID AND Quarter = @Quarter
          AND EndDate >= @FiscalYearStart AND EndDate <= @FiscalYearEnd";
            DateTime fiscalYearStart, fiscalYearEnd;// Get the fiscal year start and end dates
            GetFiscalYearStartEnd(companyId, year, connection, transaction, out fiscalYearStart, out fiscalYearEnd);
            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Quarter", quarter);
                command.Parameters.AddWithValue("@FiscalYearStart", fiscalYearStart);
                command.Parameters.AddWithValue("@FiscalYearEnd", fiscalYearEnd);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string jsonData = reader.GetString(0);
                        var financialData = ParseFinancialDataJson(jsonData);
                        if (financialData.TryGetValue(columnName, out object valueObj))
                        {
                            if (decimal.TryParse(valueObj.ToString(), out decimal value))
                            {
                                return value;
                            }
                        }
                    }
                }
            }
            return null; // Return null if value is missing
        }
        private static void GetFiscalYearStartEnd(int companyId, int year, SqlConnection connection, SqlTransaction transaction, out DateTime fiscalYearStart, out DateTime fiscalYearEnd)
        {
            string query = @"
        SELECT TOP 1 StartDate, EndDate
        FROM FinancialData
        WHERE CompanyID = @CompanyID AND Quarter = 0 AND YEAR(EndDate) = @Year
        ORDER BY EndDate DESC";
            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        fiscalYearStart = reader.GetDateTime(0);
                        fiscalYearEnd = reader.GetDateTime(1);
                        Console.WriteLine($"[DEBUG] Fiscal Year {year}: StartDate = {fiscalYearStart.ToShortDateString()}, EndDate = {fiscalYearEnd.ToShortDateString()}");
                        return;
                    }
                }
            }    // Default to calendar year if no annual report is found
            fiscalYearStart = new DateTime(year, 1, 1);
            fiscalYearEnd = new DateTime(year, 12, 31);
            Console.WriteLine($"[WARNING] No annual report found for Fiscal Year {year}. Defaulting to Calendar Year.");
        }
        private static decimal? GetNullableFinancialValue(SqlConnection connection, SqlTransaction transaction, int companyId, int year, int quarter, string columnName)
        {  // Same implementation as GetFinancialValue
            return GetFinancialValue(connection, transaction, companyId, year, quarter, columnName);
        }    
        public static (DateTime periodStart, DateTime periodEnd) GetStandardPeriodDatesBasedOnCalendarYear(int fiscalYear, int quarter)
        {
            DateTime periodStart, periodEnd;
            switch (quarter)
            {
                case 1:
                    periodStart = new DateTime(fiscalYear, 1, 1);
                    periodEnd = new DateTime(fiscalYear, 3, 31);
                    break;
                case 2:
                    periodStart = new DateTime(fiscalYear, 4, 1);
                    periodEnd = new DateTime(fiscalYear, 6, 30);
                    break;
                case 3:
                    periodStart = new DateTime(fiscalYear, 7, 1);
                    periodEnd = new DateTime(fiscalYear, 9, 30);
                    break;
                case 4:
                    periodStart = new DateTime(fiscalYear, 10, 1);
                    periodEnd = new DateTime(fiscalYear, 12, 31);
                    break;
                case 0:
                    // For annual reports, the entire calendar year
                    periodStart = new DateTime(fiscalYear, 1, 1);
                    periodEnd = new DateTime(fiscalYear, 12, 31);
                    break;
                default:
                    throw new ArgumentException("Invalid quarter value", nameof(quarter));
            }
            return (periodStart, periodEnd);
        }
        private static void InsertQ4Data(SqlConnection connection, SqlTransaction transaction, int companyId, int year, DateTime startDate, DateTime endDate, Dictionary<string, object> q4Values)
        {    // Calculate fiscal year based on endDate and quarter
            int fiscalYear = CompanyFinancialData.GetFiscalYear(endDate, 4, (DateTime?)endDate);            
            (DateTime standardStartDate, DateTime standardEndDate) = Data.GetStandardPeriodDates(fiscalYear, 4, (DateTime?)endDate);            
            var q4Entry = new FinancialDataEntry// Initialize the Q4 FinancialDataEntry with standardized dates
            {
                CompanyID = companyId,
                StartDate = startDate, // Parsed StartDate
                EndDate = endDate,     // Parsed EndDate
                Quarter = 4,
                IsHtmlParsed = true,
                IsXbrlParsed = true,
                FinancialValues = q4Values,
                StandardStartDate = standardStartDate, // Set StandardStartDate
                StandardEndDate = standardEndDate,      // Set StandardEndDate
                FiscalYearEndDate = endDate             // Set FiscalYearEndDate
            };
            var entries = new List<FinancialDataEntry> { q4Entry };
            SaveCompleteEntryToDatabase(entries, connection, transaction).Wait();
        }
        public static async Task CalculateAndSaveQ4InDatabaseAsync(SqlConnection connection, SqlTransaction transaction, int companyId, DataNonStatic dataNonStatic)
        { // Ensure the CompanyFinancialData is loaded
            var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(companyId);
            if (companyData == null)
            {
                Console.WriteLine($"[ERROR] Failed to load CompanyData for CompanyID: {companyId}. Cannot calculate Q4.");
                return;
            }   // Get the most recent fiscal year end date from the cache
            DateTime? fiscalYearEndDate = companyData.GetMostRecentFiscalYearEndDate();
            if (!fiscalYearEndDate.HasValue)
            {
                Console.WriteLine($"[ERROR] No fiscal year end date found for CompanyID: {companyId}. Cannot calculate Q4.");
                return;
            }    // Calculate fiscal year based on fiscalYearEndDate
            int fiscalYear = CompanyFinancialData.GetFiscalYear(fiscalYearEndDate.Value, 0, fiscalYearEndDate);
            // Get Standard Period Dates based on Fiscal Year and Quarter
            (DateTime fiscalYearStartDate, DateTime fiscalYearEndDateActual) = Data.GetStandardPeriodDates(fiscalYear, 0, fiscalYearEndDate);            
            DateTime q3EndDate = GetQuarterEndDate(connection, transaction, companyId, fiscalYear, 3);// Fetch Q3 End Date           
            DateTime q4StartDate; // Determine Q4 Start Date
            if (q3EndDate != DateTime.MinValue)
            {
                q4StartDate = q3EndDate.AddDays(1);
            }
            else
            {    // If Q3 End Date is missing, calculate based on fiscal year start
                TimeSpan fiscalYearDuration = fiscalYearEndDateActual - fiscalYearStartDate;
                int daysInFiscalYear = fiscalYearDuration.Days + 1;
                int daysPerQuarter = daysInFiscalYear / 4;
                q4StartDate = fiscalYearStartDate.AddDays(3 * daysPerQuarter);
            }
            DateTime q4EndDate = fiscalYearEndDateActual;
            var q4Values = new Dictionary<string, object>();
            var allElementNames = GetAllFinancialElements(connection, transaction, companyId, fiscalYear); // Get all financial element names                
            ProcessAllFinancialElements(connection, transaction, companyId, fiscalYear, q4Values, allElementNames); // Process all financial elements              
            InsertQ4Data(connection, transaction, companyId, fiscalYear, q4StartDate, q4EndDate, q4Values); // Insert Q4 data
        }
        private static List<string> GetAllFinancialElements(SqlConnection connection, SqlTransaction transaction, int companyId, int year)
        {
            var elementNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string query = @"
        SELECT FinancialDataJson
        FROM FinancialData
        WHERE CompanyID = @CompanyID AND YEAR(EndDate) = @Year";
            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string jsonData = reader.GetString(0);
                        var financialData = ParseFinancialDataJson(jsonData);
                        foreach (var key in financialData.Keys)
                        {
                            elementNames.Add(key);
                        }
                    }
                }
            }
            return elementNames.ToList();
        }
        public static void MergeEntries(this FinancialDataEntry existingEntry, FinancialDataEntry newEntry)
        {
            existingEntry.IsXbrlParsed = existingEntry.IsXbrlParsed || newEntry.IsXbrlParsed;
            existingEntry.IsHtmlParsed = existingEntry.IsHtmlParsed || newEntry.IsHtmlParsed;
            foreach (var kvp in newEntry.FinancialValues)
            {
                existingEntry.FinancialValues[kvp.Key] = kvp.Value;
                existingEntry.FinancialValueTypes[kvp.Key] = newEntry.FinancialValueTypes[kvp.Key];
            }
        }
        public static (DateTime periodStart, DateTime periodEnd) GetStandardPeriodDates(
    int fiscalYear, int quarter, DateTime? fiscalYearEndDate = null)
        {
            if (fiscalYearEndDate.HasValue)
            {
                DateTime fiscalYearStart = fiscalYearEndDate.Value.AddYears(-1).AddDays(1);
                DateTime periodStart, periodEnd;
                switch (quarter)
                {
                    case 1:
                        periodStart = fiscalYearStart;
                        periodEnd = fiscalYearStart.AddMonths(3).AddDays(-1);
                        break;
                    case 2:
                        periodStart = fiscalYearStart.AddMonths(3);
                        periodEnd = fiscalYearStart.AddMonths(6).AddDays(-1);
                        break;
                    case 3:
                        periodStart = fiscalYearStart.AddMonths(6);
                        periodEnd = fiscalYearStart.AddMonths(9).AddDays(-1);
                        break;
                    case 4:
                        periodStart = fiscalYearStart.AddMonths(9);
                        periodEnd = fiscalYearEndDate.Value;
                        break;
                    case 0:
                        periodStart = fiscalYearStart;
                        periodEnd = fiscalYearEndDate.Value;
                        break;
                    default:
                        throw new ArgumentException("Invalid quarter value", nameof(quarter));
                }
                Console.WriteLine($"[DEBUG] GetStandardPeriodDates - FiscalYear: {fiscalYear}, Quarter: {quarter}, Start: {periodStart.ToShortDateString()}, End: {periodEnd.ToShortDateString()}");
                return (periodStart, periodEnd);
            }
            else
            {    // Fallback logic based on calendar year
                var dates = GetStandardPeriodDatesBasedOnCalendarYear(fiscalYear, quarter);
                Console.WriteLine($"[DEBUG] GetStandardPeriodDatesBasedOnCalendarYear - FiscalYear: {fiscalYear}, Quarter: {quarter}, Start: {dates.periodStart.ToShortDateString()}, End: {dates.periodEnd.ToShortDateString()}");
                return dates;
            }
        }
        public static DateTime AdjustToNearestQuarterEndDate(DateTime date)
        {       // Define the possible quarter end dates
            DateTime[] quarterEndDates = new[]
            {
        new DateTime(date.Year, 3, 31),
        new DateTime(date.Year, 6, 30),
        new DateTime(date.Year, 9, 30),
        new DateTime(date.Year, 12, 31)
    };           // Find the quarter end date closest to the given date
            DateTime closestDate = quarterEndDates.OrderBy(d => Math.Abs((d - date).TotalDays)).First();
            return closestDate;
        }
        public static int GetFiscalYear(DateTime endDate, int quarter, DateTime? fiscalYearEndDate = null)
        {
            if (fiscalYearEndDate.HasValue)
            {
                DateTime fiscalYearStart = fiscalYearEndDate.Value.AddYears(-1).AddDays(1);
                if (endDate > fiscalYearEndDate.Value)
                {         // End date is after fiscal year-end, so it's in the next fiscal year
                    return fiscalYearEndDate.Value.AddYears(1).Year;
                }
                else if (endDate >= fiscalYearStart)
                {          // End date is within the fiscal year
                    return fiscalYearEndDate.Value.Year;
                }
                else
                {     // End date is before the fiscal year start, so it's in the previous fiscal year
                    return fiscalYearEndDate.Value.AddYears(-1).Year;
                }
            }
            else
            {// Fallback logic if fiscalYearEndDate is not available
                switch (quarter)
                {
                    case 1:
                        return endDate.AddMonths(-9).Year;
                    case 2:
                        return endDate.AddMonths(-6).Year;
                    case 3:
                        return endDate.AddMonths(-3).Year;
                    case 4:
                    case 0:
                        return endDate.Year;
                    default:
                        throw new ArgumentException("Invalid quarter value", nameof(quarter));
                }
            }
        }
        public static DateTime CalculateFiscalYearStartDate(DateTime fiscalYearEndDate)
        {
            DateTime fiscalYearStartDate = fiscalYearEndDate.AddDays(1).AddYears(-1);
            return fiscalYearStartDate;
        }
        public static async Task SaveCompleteEntryToDatabase(List<FinancialDataEntry> entries, SqlConnection connection, SqlTransaction transaction)
        {
            if (entries == null || !entries.Any()) return;
            DataTable table = CreateDataTable(entries, connection, transaction);
            TimeSpan leeway = TimeSpan.FromDays(15);
            DataTable existingRows = FetchExistingRows(connection, transaction, entries.First().CompanyID, table, leeway);
            // Determine rows to insert (exclude existing ones)
            var rowsToInsertEnumerable = table.AsEnumerable().Where(newRow =>
                !existingRows.AsEnumerable().Any(existingRow =>
                    existingRow.Field<int>("CompanyID") == newRow.Field<int>("CompanyID") &&
                    existingRow.Field<DateTime>("StartDate") == newRow.Field<DateTime>("StartDate") &&
                    existingRow.Field<DateTime>("EndDate") == newRow.Field<DateTime>("EndDate")));
            DataTable rowsToInsert = null;
            if (rowsToInsertEnumerable.Any())
            {
                rowsToInsert = rowsToInsertEnumerable.CopyToDataTable();
            }
            if (rowsToInsert != null && rowsToInsert.Rows.Count > 0)
            {  // Perform bulk insert for new rows
                await BulkInsert(rowsToInsert, connection, transaction);
            }   // Update the method call to include the leeway
            await UpdateExistingRecords(entries, connection, transaction, existingRows, leeway);
        }
        public static bool IsValidDateFormat(string text)
        {
            string pattern = @"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s\d{1,2},?\s?\d{4}$|" +
                             @"^\d{4}-\d{2}-\d{2}$|" +
                             @"^\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}$";
            return Regex.IsMatch(text, pattern);
        }
        private static async Task BulkInsert(DataTable table, SqlConnection connection, SqlTransaction transaction)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.DestinationTableName = "dbo.FinancialData";
                foreach (DataColumn column in table.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }
                try
                {
                    await bulkCopy.WriteToServerAsync(table);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Bulk insert failed: {ex.Message}");
                    throw;
                }
            }
        }
        private static async Task UpdateExistingRecords(List<FinancialDataEntry> entries, SqlConnection connection, SqlTransaction transaction, DataTable existingRows, TimeSpan leeway)
        {
            foreach (var entry in entries)
            {
                var existingRow = existingRows.AsEnumerable().FirstOrDefault(row =>
                    row.Field<int>("CompanyID") == entry.CompanyID &&
                    Math.Abs((row.Field<DateTime>("StartDate") - entry.StartDate).TotalDays) <= leeway.TotalDays &&
                    Math.Abs((row.Field<DateTime>("EndDate") - entry.EndDate).TotalDays) <= leeway.TotalDays);
                if (existingRow != null)
                {  // Retrieve existing FinancialDataJson from the database
                    string selectQuery = @"
                SELECT FinancialDataJson, IsHtmlParsed, IsXbrlParsed
                FROM FinancialData
                WHERE CompanyID = @CompanyID AND StartDate = @StartDate AND EndDate = @EndDate";
                    string existingJson = null;
                    bool existingIsHtmlParsed = false;
                    bool existingIsXbrlParsed = false;
                    using (SqlCommand selectCommand = new SqlCommand(selectQuery, connection, transaction))
                    {
                        selectCommand.Parameters.AddWithValue("@CompanyID", entry.CompanyID);
                        selectCommand.Parameters.AddWithValue("@StartDate", entry.StartDate);
                        selectCommand.Parameters.AddWithValue("@EndDate", entry.EndDate);
                        using (SqlDataReader reader = await selectCommand.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                existingJson = reader.GetString(0);
                                existingIsHtmlParsed = reader.GetBoolean(1);
                                existingIsXbrlParsed = reader.GetBoolean(2);
                            }
                        } // The DataReader is closed here
                    } // selectCommand is disposed here
                    if (existingJson != null)
                    {
                        var existingFinancialValues = ParseFinancialDataJson(existingJson);   // Merge the existing data with the new data
                        var mergedFinancialValues = new Dictionary<string, object>(existingFinancialValues, StringComparer.OrdinalIgnoreCase);
                        foreach (var kvp in entry.FinancialValues)
                        {
                            mergedFinancialValues[kvp.Key] = kvp.Value;
                        }      // Update the IsHtmlParsed and IsXbrlParsed flags
                        bool updatedIsHtmlParsed = existingIsHtmlParsed || entry.IsHtmlParsed;
                        bool updatedIsXbrlParsed = existingIsXbrlParsed || entry.IsXbrlParsed;
                        // Prepare update command
                        string updateQuery = @"
                    UPDATE FinancialData
                    SET FinancialDataJson = @FinancialDataJson,
                        IsHtmlParsed = @IsHtmlParsed,
                        IsXbrlParsed = @IsXbrlParsed
                    WHERE CompanyID = @CompanyID AND StartDate = @StartDate AND EndDate = @EndDate";
                        using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection, transaction))
                        {
                            updateCommand.Parameters.AddWithValue("@CompanyID", entry.CompanyID);
                            updateCommand.Parameters.AddWithValue("@StartDate", entry.StartDate);
                            updateCommand.Parameters.AddWithValue("@EndDate", entry.EndDate);
                            updateCommand.Parameters.AddWithValue("@FinancialDataJson", JsonConvert.SerializeObject(mergedFinancialValues));
                            updateCommand.Parameters.AddWithValue("@IsHtmlParsed", updatedIsHtmlParsed);
                            updateCommand.Parameters.AddWithValue("@IsXbrlParsed", updatedIsXbrlParsed);
                            try
                            {
                                await updateCommand.ExecuteNonQueryAsync();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ERROR] Update failed for CompanyID {entry.CompanyID}: {ex.Message}");
                                throw;
                            }
                        }
                    }
                }
            }
        }
        public static DataTable FetchExistingRows(SqlConnection connection, SqlTransaction transaction, int companyId, DataTable newRows, TimeSpan leeway)
        {
            DataTable existingRows = new DataTable();
            if (newRows.Rows.Count > 0)
            {
                DateTime minStartDate = newRows.AsEnumerable().Select(row => row.Field<DateTime>("StartDate")).Min().AddDays(-leeway.TotalDays);
                DateTime maxStartDate = newRows.AsEnumerable().Select(row => row.Field<DateTime>("StartDate")).Max().AddDays(leeway.TotalDays);
                DateTime minEndDate = newRows.AsEnumerable().Select(row => row.Field<DateTime>("EndDate")).Min().AddDays(-leeway.TotalDays);
                DateTime maxEndDate = newRows.AsEnumerable().Select(row => row.Field<DateTime>("EndDate")).Max().AddDays(leeway.TotalDays);
                string query = @"
            SELECT CompanyID, StartDate, EndDate
            FROM FinancialData
            WHERE CompanyID = @CompanyID
            AND StartDate BETWEEN @MinStartDate AND @MaxStartDate
            AND EndDate BETWEEN @MinEndDate AND @MaxEndDate";
                using (SqlCommand command = new SqlCommand(query, connection, transaction))
                {
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    command.Parameters.AddWithValue("@MinStartDate", minStartDate);
                    command.Parameters.AddWithValue("@MaxStartDate", maxStartDate);
                    command.Parameters.AddWithValue("@MinEndDate", minEndDate);
                    command.Parameters.AddWithValue("@MaxEndDate", maxEndDate);
                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(existingRows);
                    }
                }
            }
            return existingRows;
        }
        public static DataTable CreateDataTable(List<FinancialDataEntry> entries, SqlConnection connection, SqlTransaction transaction)
        {
            DataTable table = new DataTable();
            table.Columns.Add("CompanyID", typeof(int));
            table.Columns.Add("StartDate", typeof(DateTime)); // Use StandardStartDate
            table.Columns.Add("EndDate", typeof(DateTime));   // Use StandardEndDate
            table.Columns.Add("Quarter", typeof(int));
            table.Columns.Add("Year", typeof(int));
            table.Columns.Add("FinancialDataJson", typeof(string));
            table.Columns.Add("IsHtmlParsed", typeof(bool));
            table.Columns.Add("IsXbrlParsed", typeof(bool));
            var uniqueRows = new HashSet<(int CompanyID, DateTime StartDate, DateTime EndDate)>();
            foreach (var entry in entries)
            {
                if (entry.StandardStartDate < new DateTime(1753, 1, 1) || entry.StandardEndDate < new DateTime(1753, 1, 1))
                {
                    continue; // Skip invalid dates
                }
                var uniqueKey = (entry.CompanyID, entry.StandardStartDate, entry.StandardEndDate);
                if (uniqueRows.Contains(uniqueKey))
                {
                    continue; // Skip duplicates
                }
                uniqueRows.Add(uniqueKey);
                DataRow row = table.NewRow();
                row["CompanyID"] = entry.CompanyID;
                row["StartDate"] = entry.StandardStartDate;
                row["EndDate"] = entry.StandardEndDate;
                row["Quarter"] = entry.Quarter;   // Calculate Fiscal Year based on standardized EndDate and FiscalYearEndDate
                int fiscalYear = Data.GetFiscalYear(entry.StandardEndDate, entry.Quarter, entry.FiscalYearEndDate);
                row["Year"] = fiscalYear; // Set Year as an integer
                row["FinancialDataJson"] = JsonConvert.SerializeObject(entry.FinancialValues);
                row["IsHtmlParsed"] = entry.IsHtmlParsed;
                row["IsXbrlParsed"] = entry.IsXbrlParsed;
                table.Rows.Add(row);
            }
            return table;
        }
        public static DateTime GetFiscalYearEndForSpecificYearWithFallback(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
        {
            try
            {
                using (var command = new SqlCommand(@"
            SELECT TOP 1 EndDate
            FROM FinancialData
            WHERE CompanyID = @CompanyID AND Quarter = 0 AND EndDate IS NOT NULL
            AND YEAR(EndDate) <= @Year
            ORDER BY EndDate DESC", connection, transaction))
                {
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    command.Parameters.AddWithValue("@Year", year);
                    object result = command.ExecuteScalar();
                    if (result != null && DateTime.TryParse(result.ToString(), out DateTime fiscalYearEnd))
                    {
                        return fiscalYearEnd;
                    }
                    else
                    {       // Handle the case where no matching record is found
                        DateTime fallbackDate = new DateTime(year, 12, 31);
                        return fallbackDate;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in GetFiscalYearEndForSpecificYearWithFallback: {ex.Message}");
                throw;
            }
        }
        public static Dictionary<int, DateTime?> companyFiscalYearStartDates = new Dictionary<int, DateTime?>();
        public static int CalculateFinancialYearBasedOnQuarter(DateTime endDate, int quarter)
        {
            switch (quarter)
            {
                case 1:
                    return endDate.AddMonths(9).Year;
                case 2:
                    return endDate.AddMonths(6).Year;
                case 3:
                    return endDate.AddMonths(3).Year;
                case 4:
                case 0:
                    return endDate.Year;
                default:
                    throw new ArgumentException("Invalid quarter value", nameof(quarter));
            }
        }
        public static int CalculateQuarterByFiscalDayMonth(DateTime reportDate, DateTime fiscalYearEndDate, int leewayDays = 15)
        {
            DateTime fiscalYearStart = fiscalYearEndDate.AddYears(-1).AddDays(1);// Adjust if reportDate is after fiscalYearEndDate
            if (reportDate > fiscalYearEndDate)
            {
                fiscalYearStart = fiscalYearEndDate.AddDays(1);
                fiscalYearEndDate = fiscalYearEndDate.AddYears(1);
            }
            int daysFromFiscalStart = (reportDate - fiscalYearStart).Days;
            int totalFiscalDays = (fiscalYearEndDate - fiscalYearStart).Days + 1;
            int daysPerQuarter = totalFiscalDays / 4;
            int q1Boundary = daysPerQuarter + leewayDays;
            int q2Boundary = 2 * daysPerQuarter + leewayDays;
            int q3Boundary = 3 * daysPerQuarter + leewayDays;
            if (daysFromFiscalStart < q1Boundary)
                return 1;
            else if (daysFromFiscalStart < q2Boundary)
                return 2;
            else if (daysFromFiscalStart < q3Boundary)
                return 3;
            else
                return 4;
        }
        private static DateTime GetQuarterEndDate(SqlConnection connection, SqlTransaction transaction, int companyId, int year, int quarter)
        {
            string query = @"
SELECT EndDate
FROM FinancialData
WHERE CompanyID = @CompanyID AND YEAR(EndDate) = @Year AND Quarter = @Quarter";
            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                command.Parameters.AddWithValue("@Quarter", quarter);
                object result = command.ExecuteScalar();
                if (result != null && DateTime.TryParse(result.ToString(), out DateTime endDate))
                {
                    return endDate;
                }
                else
                {
                    return DateTime.MinValue;
                }
            }
        }
        private static SqlCommand CloneCommand(SqlCommand originalCommand)
        {
            SqlCommand clonedCommand = new SqlCommand
            {
                CommandText = originalCommand.CommandText,
                CommandType = originalCommand.CommandType
            };
            foreach (SqlParameter param in originalCommand.Parameters)
            {
                clonedCommand.Parameters.Add(((ICloneable)param).Clone());
            }
            return clonedCommand;
        }
        public class CompanyInfo
        {
            [JsonProperty("cik_str")]
            public string CIK { get; set; }

            [JsonProperty("ticker")]
            public string Ticker { get; set; }

            [JsonProperty("title")]
            public string CompanyName { get; set; }
        }
        private static readonly SemaphoreSlim batchSemaphore = new SemaphoreSlim(1, 1); // Semaphore to ensure only one batch executes at a time
        private const int batchSizeLimit = 50; // Adjust this number based on SQL Server capacity
        public static async Task ExecuteBatch(List<SqlCommand> batchedCommands, string connectionString)
        {
            const int maxRetries = 3;
            int retryCount = 0;
            bool success = false;
            while (!success && retryCount < maxRetries)
            {
                try
                {
                    if (batchedCommands.Count > 0)
                    {
                        await batchSemaphore.WaitAsync();
                        try
                        {
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                await connection.OpenAsync();
                                foreach (var batch in batchedCommands.Batch(batchSizeLimit))
                                {
                                    using (var transaction = connection.BeginTransaction())
                                    {
                                        try
                                        {
                                            foreach (var command in batch)
                                            {
                                                using (var clonedCommand = CloneCommand(command))
                                                {
                                                    clonedCommand.Connection = connection;
                                                    clonedCommand.Transaction = transaction;
                                                    await clonedCommand.ExecuteNonQueryAsync();
                                                }
                                            }
                                            transaction.Commit();
                                        }
                                        catch (Exception ex)
                                        {
                                            transaction.Rollback();
                                            throw;
                                        }
                                    }
                                }
                                batchedCommands.Clear();
                                success = true;
                            }
                        }
                        finally
                        {
                            batchSemaphore.Release();
                        }
                    }
                    else
                    {
                        success = true;
                    }
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount >= maxRetries) throw;
                }
            }
        }
        public static Dictionary<string, object> ParseFinancialDataJson(string jsonData)
        {
            if (string.IsNullOrWhiteSpace(jsonData))
            {
                return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            }
            var tempData = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonData);
            return new Dictionary<string, object>(tempData, StringComparer.OrdinalIgnoreCase);
        }
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items, int batchSize)
        {
            return items.Select((item, inx) => new { item, inx })
                        .GroupBy(x => x.inx / batchSize)
                        .Select(g => g.Select(x => x.item));
        }
    }
}


//using DataElements;
//using System;
//using System.Collections.Generic;
//using System.Data.SqlClient;
//using System.Diagnostics;
//using System.Linq;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;
//using static DataElements.FinancialElementLists;
//using System.Xml.Linq;
//using Newtonsoft.Json;
//using System.Collections.Concurrent;
//using System.Data;

//namespace Data
//{
//    public static class Data
//    {
//        public static Dictionary<int, DateTime?> companyFiscalYearStartDates = new Dictionary<int, DateTime?>();
//        public static int CalculateQuarterByFiscalDayMonth(int companyId, DateTime reportDate, int fiscalStartMonth = 7, int fiscalStartDay = 1, int leewayDays = 15)
//        {
//            DateTime? companyFiscalYearStartDate;
//            if (companyFiscalYearStartDates.TryGetValue(companyId, out companyFiscalYearStartDate) && companyFiscalYearStartDate.HasValue)
//            {
//                fiscalStartMonth = companyFiscalYearStartDate.Value.Month;
//                fiscalStartDay = companyFiscalYearStartDate.Value.Day;
//            }
//            else
//            {
//                companyFiscalYearStartDates[companyId] = new DateTime(reportDate.Year, fiscalStartMonth, fiscalStartDay);
//            }
//            DateTime fiscalStart = new DateTime(1, fiscalStartMonth, fiscalStartDay);// Create a fiscal start date ignoring the year
//            DateTime normalizedReportDate = new DateTime(1, reportDate.Month, reportDate.Day);// Normalize the report date to the same arbitrary year (ignoring the year)           
//            if (normalizedReportDate < fiscalStart)// Explicitly handle the wrap-around case where the report date comes before the fiscal start
//            {
//                fiscalStart = fiscalStart.AddYears(-1);
//            }
//            int daysFromFiscalStart = (normalizedReportDate - fiscalStart).Days;
//            int q1Boundary = 90 + leewayDays;
//            int q2Boundary = 180 + leewayDays;
//            int q3Boundary = 270 + leewayDays;
//            if (daysFromFiscalStart < q1Boundary) return 1;
//            if (daysFromFiscalStart < q2Boundary) return 2;
//            if (daysFromFiscalStart < q3Boundary) return 3;
//            return 4;
//        }
//        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, bool isHtmlParsed, bool isXbrlParsed, List<SqlCommand> batchedCommands, int leewayDays = 15)
//        {
//            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//            {
//                connection.Open();
//                SqlTransaction transaction = connection.BeginTransaction();
//                try
//                {
//                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
//                    {// Step 1: Retrieve Company details (Name and Symbol)
//                        command.CommandText = @"SELECT CompanyName, CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
//                        command.Parameters.AddWithValue("@CompanyID", companyId);
//                        using (var reader = command.ExecuteReader())
//                        {
//                            if (!reader.Read())// No matching company found
//                            {
//                                return;
//                            }
//                            reader.Close();
//                        }
//                        int fiscalMonth = 1;
//                        int fiscalDay = 1;
//                        // Step 2: Retrieve the most recent annual report's fiscal start month and day (no fixed year)
//                        // Check if fiscal year start date is already stored
//                        if (!companyFiscalYearStartDates.TryGetValue(companyId, out DateTime? fiscalYearStartDate) || !fiscalYearStartDate.HasValue)
//                        { // Retrieve the most recent annual report's fiscal start month and day (no fixed year)
//                            command.CommandText = @"
//        SELECT TOP 1 MONTH(StartDate) AS FiscalMonth, DAY(StartDate) AS FiscalDay
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID 
//        AND Quarter = 0  -- Annual reports only
//        AND ABS(DATEDIFF(DAY, StartDate, EndDate) - 365) <= 20
//        ORDER BY EndDate DESC";
//                            using (var fiscalReader = command.ExecuteReader())
//                            {
//                                if (fiscalReader.Read())
//                                {
//                                    fiscalMonth = fiscalReader.GetInt32(0);
//                                    fiscalDay = fiscalReader.GetInt32(1);
//                                    fiscalYearStartDate = new DateTime(DateTime.Now.Year, fiscalMonth, fiscalDay);
//                                    companyFiscalYearStartDates[companyId] = fiscalYearStartDate; // Store it in the dictionary for future use
//                                }
//                                fiscalReader.Close();
//                            }
//                            if (!fiscalYearStartDate.HasValue)// Default to January 1st if nothing is found (consider handling this scenario properly)
//                            {
//                                fiscalYearStartDate = new DateTime(DateTime.Now.Year, 1, 1);
//                                companyFiscalYearStartDates[companyId] = fiscalYearStartDate;
//                            }
//                        }
//                        if (fiscalYearStartDate.HasValue)
//                        {
//                            fiscalYearStartDate = new DateTime(endDate.Year, fiscalMonth, fiscalDay);
//                            if (fiscalYearStartDate > endDate)
//                            {
//                                fiscalYearStartDate = fiscalYearStartDate.Value.AddYears(-1);
//                            }
//                        }
//                        if (quarter != 0 && fiscalYearStartDate.HasValue)
//                        {
//                            int daysDifference = (fiscalYearStartDate.Value - endDate).Days;
//                            if (daysDifference > leewayDays)
//                            {
//                                fiscalYearStartDate = fiscalYearStartDate.Value.AddYears(-1);
//                            }
//                            int reportMonth = endDate.Month;
//                            int reportDay = endDate.Day;
//                            quarter = CalculateQuarterByFiscalDayMonth(companyId, endDate, fiscalMonth, fiscalDay, leewayDays);
//                        }
//                        int financialYear = (quarter == 0) ? endDate.Year : Nasdaq100FinancialScraper.Program.CalculateFinancialYear(companyId, endDate, connection, transaction);
//                        DateTime startDate = (quarter == 0) ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);
//                        int dateDifference = (endDate - startDate).Days;
//                        // Step 8: Check for existing rows using CompanyID, StartDate, and EndDate (with leeway)
//                        command.CommandText = @"
//                SELECT COUNT(*) 
//                FROM FinancialData 
//                WHERE CompanyID = @CompanyID 
//                AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
//                AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
//                        command.Parameters.Clear();
//                        command.Parameters.AddWithValue("@CompanyID", companyId);
//                        command.Parameters.AddWithValue("@StartDate", startDate);
//                        command.Parameters.AddWithValue("@EndDate", endDate);
//                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);
//                        int existingRowCount = (int)command.ExecuteScalar();
//                        bool hasExistingRow = existingRowCount > 0;
//                        if (hasExistingRow)// Step 9: Use FinancialYear for updating or inserting rows in FinancialData
//                        {
//                            command.CommandText = @"
//                    UPDATE FinancialData 
//                    SET 
//                        FinancialYear = CASE WHEN @FinancialYear IS NOT NULL THEN @FinancialYear ELSE FinancialYear END, 
//                        StartDate = CASE WHEN @StartDate IS NOT NULL THEN @StartDate ELSE StartDate END, 
//                        EndDate = CASE WHEN @EndDate IS NOT NULL THEN @EndDate ELSE EndDate END
//                    WHERE CompanyID = @CompanyID 
//                    AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
//                    AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
//                        }
//                        else
//                        {
//                            command.CommandText = @"
//                    INSERT INTO FinancialData (CompanyID, FinancialYear, Year, Quarter, StartDate, EndDate)
//                    VALUES (@CompanyID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate)";
//                        }
//                        command.Parameters.Clear();
//                        command.Parameters.AddWithValue("@CompanyID", companyId);
//                        command.Parameters.AddWithValue("@FinancialYear", financialYear);
//                        command.Parameters.AddWithValue("@Year", endDate.Year);
//                        command.Parameters.AddWithValue("@Quarter", quarter);
//                        command.Parameters.AddWithValue("@StartDate", startDate);
//                        command.Parameters.AddWithValue("@EndDate", endDate);
//                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);
//                        batchedCommands.Add(CloneCommand(command));
//                        if (elementName != "AnnualReport" && elementName != "QuarterlyReport")
//                        {
//                            string columnName = Nasdaq100FinancialScraper.Program.GetColumnName(elementName);
//                            if (!string.IsNullOrEmpty(columnName))
//                            {
//                                command.CommandText = $@"
//                        UPDATE FinancialData 
//                        SET [{columnName}] = @Value
//                        WHERE CompanyID = @CompanyID 
//                        AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
//                        AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
//                                command.Parameters.Clear();
//                                command.Parameters.AddWithValue("@CompanyID", companyId);
//                                command.Parameters.AddWithValue("@StartDate", startDate);
//                                command.Parameters.AddWithValue("@EndDate", endDate);
//                                command.Parameters.AddWithValue("@Value", value);
//                                command.Parameters.AddWithValue("@LeewayDays", leewayDays);
//                                batchedCommands.Add(CloneCommand(command));
//                            }
//                        }
//                        if (isHtmlParsed || isXbrlParsed)
//                        {
//                            command.CommandText = @"
//                    UPDATE FinancialData 
//                    SET ParsedFullHTML = CASE WHEN @ParsedFullHTML = 'Yes' THEN 'Yes' ELSE ParsedFullHTML END,
//                        ParsedFullXBRL = CASE WHEN @ParsedFullXBRL = 'Yes' THEN 'Yes' ELSE ParsedFullXBRL END
//                    WHERE CompanyID = @CompanyID 
//                    AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
//                    AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
//                            command.Parameters.Clear();
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@StartDate", startDate);
//                            command.Parameters.AddWithValue("@EndDate", endDate);
//                            command.Parameters.AddWithValue("@ParsedFullHTML", isHtmlParsed ? "Yes" : DBNull.Value);
//                            command.Parameters.AddWithValue("@ParsedFullXBRL", isXbrlParsed ? "Yes" : DBNull.Value);
//                            command.Parameters.AddWithValue("@LeewayDays", leewayDays);
//                            batchedCommands.Add(CloneCommand(command));
//                        }
//                        transaction.Commit();
//                    }
//                }
//                catch (Exception ex)
//                {
//                    transaction.Rollback();
//                }
//            }
//        }
//        public static void SaveToDatabase(
//            string elementName,
//            string value,
//            XElement? context,
//            string[] elementsOfInterest,
//            List<FinancialElement> elements,
//            bool isAnnualReport,
//            string companyName,
//            string companySymbol,
//            DateTime? startDate,
//            DateTime? endDate,
//            bool isHtmlParsed = false,
//            bool isXbrlParsed = false,
//            int leewayDays = 15)
//        {
//            try
//            {
//                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//                {
//                    connection.Open();
//                    using (var transaction = connection.BeginTransaction())
//                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
//                    {
//                        try
//                        {
//                            // Insert company if not exists
//                            command.CommandText = @"
//IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
//BEGIN
//    INSERT INTO CompaniesList (CompanyName, CompanySymbol, CompanyStockExchange, Industry, Sector)
//    VALUES (@CompanyName, @CompanySymbol, @CompanyStockExchange, @Industry, @Sector);
//END";
//                            command.Parameters.AddWithValue("@CompanyName", companyName);
//                            command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
//                            command.Parameters.AddWithValue("@CompanyStockExchange", "NASDAQ");
//                            command.Parameters.AddWithValue("@Industry", "Technology");
//                            command.Parameters.AddWithValue("@Sector", "Consumer Electronics");
//                            command.ExecuteNonQuery();

//                            // Get CompanyID
//                            command.CommandText = "SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
//                            int companyId = (int)command.ExecuteScalar();

//                            if (!startDate.HasValue || !endDate.HasValue)
//                            {
//                                throw new Exception("StartDate or EndDate is not provided.");
//                            }

//                            // Step 3: Retrieve fiscal month and day
//                            int fiscalMonth = 1;
//                            int fiscalDay = 1;
//                            if (!companyFiscalYearStartDates.TryGetValue(companyId, out DateTime? fiscalYearStartDate) || !fiscalYearStartDate.HasValue)
//                            {
//                                command.CommandText = @"
//SELECT TOP 1 MONTH(StartDate) AS FiscalMonth, DAY(StartDate) AS FiscalDay
//FROM FinancialData 
//WHERE CompanyID = @CompanyID 
//AND Quarter = 0  -- Annual reports only
//ORDER BY EndDate DESC";
//                                command.Parameters.Clear();
//                                command.Parameters.AddWithValue("@CompanyID", companyId);
//                                using (var fiscalReader = command.ExecuteReader())
//                                {
//                                    if (fiscalReader.Read())
//                                    {
//                                        fiscalMonth = fiscalReader.GetInt32(0);
//                                        fiscalDay = fiscalReader.GetInt32(1);
//                                        fiscalYearStartDate = new DateTime(DateTime.Now.Year, fiscalMonth, fiscalDay);
//                                        companyFiscalYearStartDates[companyId] = fiscalYearStartDate; // Store in dictionary
//                                    }
//                                    fiscalReader.Close();
//                                }
//                                if (!fiscalYearStartDate.HasValue)
//                                {
//                                    fiscalYearStartDate = new DateTime(DateTime.Now.Year, 1, 1);
//                                    companyFiscalYearStartDates[companyId] = fiscalYearStartDate;
//                                }
//                            }
//                            else
//                            {
//                                fiscalMonth = fiscalYearStartDate.Value.Month;
//                                fiscalDay = fiscalYearStartDate.Value.Day;
//                            }

//                            int quarter;
//                            if (isAnnualReport)
//                            {
//                                quarter = 0;
//                            }
//                            else
//                            {
//                                quarter = CalculateQuarterByFiscalDayMonth(companyId, endDate.Value, fiscalMonth, fiscalDay, leewayDays);
//                            }

//                            int year = endDate.Value.Year;
//                            if (!isAnnualReport)
//                            {
//                                startDate = endDate.Value.AddMonths(-3).AddDays(1);
//                            }

//                            // Get or insert PeriodID
//                            command.CommandText = @"
//DECLARE @ExistingPeriodID INT;
//SET @ExistingPeriodID = (SELECT TOP 1 PeriodID FROM Periods WHERE Year = @Year AND Quarter = @Quarter);
//IF @ExistingPeriodID IS NOT NULL
//BEGIN
//    SELECT @ExistingPeriodID;
//END
//ELSE
//BEGIN
//    INSERT INTO Periods (Year, Quarter) VALUES (@Year, @Quarter);
//    SELECT SCOPE_IDENTITY();
//END";
//                            command.Parameters.Clear();
//                            command.Parameters.AddWithValue("@Year", year);
//                            command.Parameters.AddWithValue("@Quarter", quarter);
//                            object periodIdObject = command.ExecuteScalar();

//                            // If PeriodID could not be generated, rollback and stop the process
//                            if (periodIdObject == null || periodIdObject == DBNull.Value || Convert.ToInt32(periodIdObject) <= 0)
//                            {
//                                transaction.Rollback();
//                                return; // Stop processing if PeriodID is invalid
//                            }

//                            int periodId = Convert.ToInt32(periodIdObject);

//                            // Ensure at least one financial field or parsed flag is being updated
//                            bool hasValidFinancialData = !string.IsNullOrEmpty(value) && decimal.TryParse(value, out _);
//                            bool hasOnlyParsedData = !hasValidFinancialData && (isHtmlParsed || isXbrlParsed);

//                            // If there is no valid financial data and no parsed flags, don't insert
//                            if (!hasValidFinancialData && !hasOnlyParsedData)
//                            {
//                                transaction.Rollback();
//                                return; // No valid data to save, don't insert row
//                            }

//                            // Save or update the data
//                            command.CommandText = $@"
//IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
//BEGIN
//    UPDATE FinancialData
//    SET [{elementName}] = @Value,
//        ParsedFullHTML = CASE WHEN @IsHtmlParsed = 1 THEN 'Yes' ELSE ParsedFullHTML END,
//        ParsedFullXBRL = CASE WHEN @IsXbrlParsed = 1 THEN 'Yes' ELSE ParsedFullXBRL END
//    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
//END
//ELSE
//BEGIN
//    INSERT INTO FinancialData (CompanyID, PeriodID, Year, Quarter, StartDate, EndDate, [{elementName}], ParsedFullHTML, ParsedFullXBRL)
//    VALUES (@CompanyID, @PeriodID, @Year, @Quarter, @StartDate, @EndDate, @Value,
//            CASE WHEN @IsHtmlParsed = 1 THEN 'Yes' ELSE NULL END,
//            CASE WHEN @IsXbrlParsed = 1 THEN 'Yes' ELSE NULL END);
//END";
//                            command.Parameters.Clear();
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@PeriodID", periodId);
//                            command.Parameters.AddWithValue("@Year", year);
//                            command.Parameters.AddWithValue("@Quarter", quarter);
//                            command.Parameters.AddWithValue("@StartDate", startDate.Value.Date);
//                            command.Parameters.AddWithValue("@EndDate", endDate.Value.Date);
//                            command.Parameters.AddWithValue("@IsHtmlParsed", isHtmlParsed ? 1 : 0);
//                            command.Parameters.AddWithValue("@IsXbrlParsed", isXbrlParsed ? 1 : 0);
//                            command.Parameters.AddWithValue("@Value", hasValidFinancialData ? Convert.ToDecimal(value) : (object)DBNull.Value);
//                            command.ExecuteNonQuery();

//                            // Commit the transaction
//                            RemoveEmptyFinancialDataRows(companyId, connection, transaction);
//                            transaction.Commit();
//                        }
//                        catch (Exception ex)
//                        {
//                            // Rollback the transaction if any errors occurred
//                            transaction.Rollback();
//                            throw;
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                throw new Exception($"Failed to process data for {companyName}: {ex.Message}", ex);
//            }
//        }
//        public static void RemoveEmptyFinancialDataRows(int companyId, SqlConnection connection, SqlTransaction transaction)
//        {
//            // Combine financial columns from all three lists: ElementsOfInterest, InstantDateElements, and HTMLElementsOfInterest
//            var financialColumns = new HashSet<string>(FinancialElementLists.ElementsOfInterest);
//            financialColumns.UnionWith(FinancialElementLists.InstantDateElements);
//            financialColumns.UnionWith(FinancialElementLists.HTMLElementsOfInterest.Values.Select(v => v.ColumnName));

//            // Create a dynamic SQL query that checks if all relevant financial columns are NULL
//            string financialColumnsCheck = string.Join(" IS NULL AND ", financialColumns) + " IS NULL";
//            string sqlCommand = $@"
//        DELETE FROM FinancialData
//        WHERE CompanyID = @CompanyID
//        AND {financialColumnsCheck}
//        ";
//            using (SqlCommand command = new SqlCommand(sqlCommand, connection, transaction))
//            {
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.ExecuteNonQuery();
//            }
//        }
//        public static void MergeDuplicateFinancialDataRecords(int leewayDays = 15)
//        {
//            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//            {
//                connection.Open();
//                SqlTransaction transaction = connection.BeginTransaction();
//                try
//                {
//                    // Step 1: Identify duplicate groups
//                    string duplicateGroupsQuery = @"
//            SELECT CompanyID, Quarter, 
//                   MIN(StartDate) AS StartDate, MAX(EndDate) AS EndDate,
//                   COUNT(*) AS DuplicateCount
//            FROM FinancialData
//            GROUP BY CompanyID, Quarter, YEAR(StartDate), YEAR(EndDate)
//            HAVING COUNT(*) > 1";

//                    DataTable duplicateGroups = new DataTable();
//                    using (SqlCommand command = new SqlCommand(duplicateGroupsQuery, connection, transaction))
//                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
//                    {
//                        adapter.Fill(duplicateGroups);
//                    }

//                    // Step 2: Process each duplicate group
//                    foreach (DataRow row in duplicateGroups.Rows)
//                    {
//                        int companyId = (int)row["CompanyID"];
//                        int quarter = (int)row["Quarter"];
//                        DateTime startDate = (DateTime)row["StartDate"];
//                        DateTime endDate = (DateTime)row["EndDate"];

//                        // Step 3: Get all duplicate records for this group
//                        string duplicatesQuery = @"
//                SELECT FinancialDataID, StartDate, EndDate, *
//                FROM FinancialData
//                WHERE CompanyID = @CompanyID AND Quarter = @Quarter
//                AND DATEDIFF(DAY, @StartDate, StartDate) BETWEEN -@LeewayDays AND @LeewayDays
//                AND DATEDIFF(DAY, @EndDate, EndDate) BETWEEN -@LeewayDays AND @LeewayDays";

//                        List<DataRow> duplicateRecords = new List<DataRow>();
//                        using (SqlCommand command = new SqlCommand(duplicatesQuery, connection, transaction))
//                        {
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@Quarter", quarter);
//                            command.Parameters.AddWithValue("@StartDate", startDate);
//                            command.Parameters.AddWithValue("@EndDate", endDate);
//                            command.Parameters.AddWithValue("@LeewayDays", leewayDays);

//                            using (SqlDataAdapter adapter = new SqlDataAdapter(command))
//                            {
//                                DataTable duplicatesTable = new DataTable();
//                                adapter.Fill(duplicatesTable);
//                                duplicateRecords.AddRange(duplicatesTable.AsEnumerable());
//                            }
//                        }

//                        if (duplicateRecords.Count > 1)
//                        {
//                            // Step 4: Merge data
//                            DataRow mergedRecord = MergeFinancialDataRecords(duplicateRecords);

//                            // Step 5: Delete old records
//                            string deleteQuery = @"
//                    DELETE FROM FinancialData
//                    WHERE FinancialDataID IN ({0})";

//                            string idsToDelete = string.Join(",", duplicateRecords.Select(r => r["FinancialDataID"].ToString()));
//                            using (SqlCommand deleteCommand = new SqlCommand(string.Format(deleteQuery, idsToDelete), connection, transaction))
//                            {
//                                deleteCommand.ExecuteNonQuery();
//                            }

//                            // Step 6: Insert merged record
//                            InsertMergedFinancialDataRecord(mergedRecord, connection, transaction);
//                        }
//                    }

//                    // Commit transaction
//                    transaction.Commit();
//                }
//                catch (Exception ex)
//                {
//                    transaction.Rollback();
//                    Console.WriteLine($"Error merging duplicates: {ex.Message}");
//                    throw;
//                }
//            }
//        }

//        private static DataRow MergeFinancialDataRecords(List<DataRow> records)
//        {
//            // Assume all records have the same CompanyID, Quarter, StartDate, EndDate
//            DataRow baseRecord = records[0];

//            foreach (DataRow record in records.Skip(1))
//            {
//                foreach (DataColumn column in baseRecord.Table.Columns)
//                {
//                    if (column.ColumnName == "CompanyID" ||
//                        column.ColumnName == "Year" ||
//                        column.ColumnName == "Quarter" ||
//                        column.ColumnName == "StartDate" ||
//                        column.ColumnName == "EndDate")
//                    {
//                        continue; // Skip key columns
//                    }

//                    object baseValue = baseRecord[column];
//                    object newValue = record[column];

//                    if (baseValue == DBNull.Value && newValue != DBNull.Value)
//                    {
//                        baseRecord[column] = newValue;
//                    }
//                    else if (baseValue != DBNull.Value && newValue != DBNull.Value)
//                    {
//                        // Handle conflicts if needed
//                        // For now, we'll keep the existing value
//                    }
//                }
//            }

//            return baseRecord;
//        }

//        private static void InsertMergedFinancialDataRecord(DataRow record, SqlConnection connection, SqlTransaction transaction)
//        {
//            // Build the INSERT statement dynamically based on columns
//            StringBuilder columnsBuilder = new StringBuilder();
//            StringBuilder valuesBuilder = new StringBuilder();

//            foreach (DataColumn column in record.Table.Columns)
//            {
//                if (column.ColumnName == "FinancialDataID")
//                {
//                    continue; // Skip the ID column
//                }

//                columnsBuilder.Append($"[{column.ColumnName}],");
//                valuesBuilder.Append($"@{column.ColumnName},");
//            }

//            // Remove trailing commas
//            string columns = columnsBuilder.ToString().TrimEnd(',');
//            string values = valuesBuilder.ToString().TrimEnd(',');

//            string insertQuery = $"INSERT INTO FinancialData ({columns}) VALUES ({values})";

//            using (SqlCommand command = new SqlCommand(insertQuery, connection, transaction))
//            {
//                foreach (DataColumn column in record.Table.Columns)
//                {
//                    if (column.ColumnName == "FinancialDataID")
//                    {
//                        continue; // Skip the ID column
//                    }

//                    object value = record[column];
//                    command.Parameters.AddWithValue($"@{column.ColumnName}", value ?? DBNull.Value);
//                }

//                command.ExecuteNonQuery();
//            }
//        }

//        private static Dictionary<int, Dictionary<int, HashSet<int>>> parsedQuarterTracker = new Dictionary<int, Dictionary<int, HashSet<int>>>();

//        private static void ResetQuarterTracking(int companyId, int year)
//        {
//            if (parsedQuarterTracker.ContainsKey(companyId))
//            {
//                if (parsedQuarterTracker[companyId].ContainsKey(year))
//                {
//                    parsedQuarterTracker[companyId][year].Clear();
//                }
//            }
//        }
//        public static void CalculateAndSaveQ4InDatabase(SqlConnection connection, int companyId)
//        {
//            string fetchYearsQuery = @"
//    SELECT DISTINCT Year, EndDate
//    FROM FinancialData
//    WHERE CompanyID = @CompanyID AND Quarter = 0;";
//            List<(int year, DateTime endDate)> annualReports = new List<(int, DateTime)>();
//            using (SqlCommand command = new SqlCommand(fetchYearsQuery, connection))
//            {
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                using (SqlDataReader reader = command.ExecuteReader())
//                {
//                    while (reader.Read())
//                    {
//                        annualReports.Add((reader.GetInt32(0), reader.GetDateTime(1)));
//                    }
//                }
//            }
//            foreach (var (year, endDate) in annualReports)
//            {
//                foreach (var element in FinancialElementLists.HTMLElementsOfInterest)
//                {
//                    string columnName = element.Value.ColumnName;
//                    bool isShares = element.Value.IsShares;
//                    bool isCashFlowStatement = element.Value.IsCashFlowStatement;
//                    bool isBalanceSheet = element.Value.IsBalanceSheet;
//                    try
//                    {
//                        // Build the dynamic SQL update statement for Q4 calculation
//                        string q4CalculationQuery = $@"
//    UPDATE FD4
//    SET FD4.[{columnName}] = 
//        CASE 
//            WHEN @IsShares = 1 THEN FD1.[{columnName}] -- Use annual value for shares
//            WHEN @IsCashFlowStatement = 1 THEN FD1.[{columnName}] - COALESCE(FD3.[{columnName}], 0) -- Annual minus Q3 for cash flow
//            WHEN @IsBalanceSheet = 1 THEN FD1.[{columnName}] -- Use annual value for balance sheet elements
//            ELSE FD1.[{columnName}] - COALESCE(FD2.[{columnName}], 0) - COALESCE(FD3.[{columnName}], 0) - COALESCE(FD4.[{columnName}], 0) -- Standard Q4 calculation
//        END,
//        FD4.FinancialYear = FD1.FinancialYear -- Set FinancialYear from the annual report
//    FROM FinancialData FD1
//    LEFT JOIN FinancialData FD2 ON FD1.CompanyID = FD2.CompanyID AND FD1.Year = FD2.Year AND FD2.Quarter = 1
//    LEFT JOIN FinancialData FD3 ON FD1.CompanyID = FD3.CompanyID AND FD1.Year = FD3.Year AND FD3.Quarter = 2
//    LEFT JOIN FinancialData FD4 ON FD1.CompanyID = FD4.CompanyID AND FD1.Year = FD4.Year AND FD4.Quarter = 4
//    WHERE FD1.Quarter = 0 AND FD1.CompanyID = @CompanyID AND FD1.Year = @Year
//    AND ABS(DATEDIFF(day, FD1.EndDate, FD4.EndDate)) <= @LeewayDays;";  // Leeway for matching end dates
//                        using (SqlCommand command = new SqlCommand(q4CalculationQuery, connection))
//                        {
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@Year", year);
//                            command.Parameters.AddWithValue("@IsShares", isShares ? 1 : 0);
//                            command.Parameters.AddWithValue("@IsCashFlowStatement", isCashFlowStatement ? 1 : 0);
//                            command.Parameters.AddWithValue("@IsBalanceSheet", isBalanceSheet ? 1 : 0);
//                            command.Parameters.AddWithValue("@LeewayDays", 15);  // Allow 5 days leeway for matching dates
//                            int rowsAffected = command.ExecuteNonQuery();
//                        }
//                    }
//                    catch (Exception ex)
//                    {
//                    }
//                }
//                string updateParsedFieldsQuery = @"
//    UPDATE FinancialData 
//    SET ParsedFullHTML = CASE WHEN @ParsedFullHTML = 'Yes' THEN 'Yes' ELSE ParsedFullHTML END,
//        ParsedFullXBRL = CASE WHEN @ParsedFullXBRL = 'Yes' THEN 'Yes' ELSE ParsedFullXBRL END
//    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 4;";
//                using (SqlCommand command = new SqlCommand(updateParsedFieldsQuery, connection))
//                {
//                    command.Parameters.AddWithValue("@CompanyID", companyId);
//                    command.Parameters.AddWithValue("@Year", year);
//                    command.Parameters.AddWithValue("@ParsedFullHTML", "Yes");
//                    command.Parameters.AddWithValue("@ParsedFullXBRL", "Yes");
//                    int parsedRowsAffected = command.ExecuteNonQuery();
//                }
//            }
//        }




//        public static void CalculateQ4InDatabase(SqlConnection connection, int companyId)
//        {
//            foreach (var element in FinancialElementLists.ElementsOfInterest) // Iterate through each element in the FinancialElementLists.ElementsOfInterest
//            {
//                try
//                {
//                    // Build the dynamic SQL update statement for Q4 calculation
//                    string q4CalculationQuery = $@"
//            UPDATE FD4
//            SET FD4.[{element}] = 
//                (FD1.[{element}] - COALESCE(FD2.[{element}], 0) - COALESCE(FD3.[{element}], 0))
//            FROM FinancialData FD1
//            LEFT JOIN FinancialData FD2 ON FD1.CompanyID = FD2.CompanyID AND FD1.Year = FD2.Year AND FD2.Quarter = 1
//            LEFT JOIN FinancialData FD3 ON FD1.CompanyID = FD3.CompanyID AND FD1.Year = FD3.Year AND FD3.Quarter = 2
//            LEFT JOIN FinancialData FD4 ON FD1.CompanyID = FD4.CompanyID AND FD1.Year = FD4.Year AND FD4.Quarter = 4
//            WHERE FD1.Quarter = 0 AND FD1.CompanyID = @CompanyID;";
//                    using (SqlCommand command = new SqlCommand(q4CalculationQuery, connection))
//                    {
//                        command.Parameters.AddWithValue("@CompanyID", companyId);
//                        int rowsAffected = command.ExecuteNonQuery();
//                    }
//                }
//                catch (Exception ex)
//                {
//                }
//            }
//        }
//        public static decimal? GetQuarterData(int companyId, int year, int quarter, string elementName, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = $@"
//            SELECT TOP 1 [{elementName}]
//            FROM FinancialData
//            WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", quarter);
//                object result = command.ExecuteScalar();
//                if (result != null && decimal.TryParse(result.ToString(), out decimal quarterValue))
//                {
//                    return quarterValue;
//                }
//                else
//                {
//                    return null;
//                }
//            }
//        }
//        private static void MarkQuarterAsParsed(int companyId, int year, int quarter)
//        {
//            if (!parsedQuarterTracker.ContainsKey(companyId))
//            {
//                parsedQuarterTracker[companyId] = new Dictionary<int, HashSet<int>>();
//            }
//            if (!parsedQuarterTracker[companyId].ContainsKey(year))
//            {
//                parsedQuarterTracker[companyId][year] = new HashSet<int>();
//            }
//            parsedQuarterTracker[companyId][year].Add(quarter);
//        }
//        private static bool AreAllQuartersParsed(int companyId, int year)
//        {
//            return parsedQuarterTracker.ContainsKey(companyId) &&
//                   parsedQuarterTracker[companyId].ContainsKey(year) &&
//                   parsedQuarterTracker[companyId][year].Contains(1) &&
//                   parsedQuarterTracker[companyId][year].Contains(2) &&
//                   parsedQuarterTracker[companyId][year].Contains(3);
//        }

//        private static SqlCommand CloneCommand(SqlCommand originalCommand)
//        {
//            SqlCommand clonedCommand = new SqlCommand
//            {
//                CommandText = originalCommand.CommandText,
//                CommandType = originalCommand.CommandType
//            };
//            foreach (SqlParameter param in originalCommand.Parameters)
//            {
//                clonedCommand.Parameters.Add(((ICloneable)param).Clone());
//            }
//            return clonedCommand;
//        }
//        public static async Task ProcessUnfinishedRows()
//        {
//            // Initialize the list to store batched SQL commands
//            List<SqlCommand> batchedCommands = new List<SqlCommand>();

//            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//            {
//                await connection.OpenAsync();
//                string query = @"
//        SELECT CompanyID, PeriodID, Year, Quarter, ParsedFullHTML, ParsedFullXBRL 
//        FROM FinancialData 
//        WHERE ParsedFullHTML IS NULL OR ParsedFullXBRL IS NULL 
//        OR ParsedFullHTML != 'Yes' OR ParsedFullXBRL != 'Yes'";

//                using (SqlCommand command = new SqlCommand(query, connection))
//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    var tasks = new List<Task>();

//                    while (await reader.ReadAsync())
//                    {
//                        int companyId = reader.GetInt32(0);
//                        int periodId = reader.GetInt32(1);
//                        int year = reader.GetInt32(2);
//                        int quarter = reader.GetInt32(3);
//                        bool needsHtmlParsing = reader.IsDBNull(4) || reader.GetString(4) != "Yes";
//                        bool needsXbrlParsing = reader.IsDBNull(5) || reader.GetString(5) != "Yes";
//                        string companyName = await StockScraperV3.URL.GetCompanyName(companyId);
//                        string companySymbol = await StockScraperV3.URL.GetCompanySymbol(companyId);

//                        tasks.Add(Task.Run(async () =>
//                        {
//                            if (needsHtmlParsing)
//                            {
//                                // Pass the batchedCommands list to the HTML reparse method
//                                // Updated method call without year and quarter
//                                await HTML.HTML.ReparseHtmlReports(companyId, periodId, companyName, companySymbol, batchedCommands);

//                            }

//                            if (needsXbrlParsing)
//                            {
//                                // Pass the batchedCommands list to the XBRL reparse method
//                                // Updated method call without year and quarter
//                                await XBRL.XBRL.ReparseXbrlReports(companyId, periodId, companyName, companySymbol, batchedCommands);

//                            }
//                        }));
//                    }

//                    await Task.WhenAll(tasks);
//                }
//            }

//            // Pass the batched commands to ExecuteBatch
//            await ExecuteBatch(batchedCommands);
//        }

//        public class CompanyInfo
//        {
//            [JsonProperty("cik_str")]
//            public string CIK { get; set; }

//            [JsonProperty("ticker")]
//            public string Ticker { get; set; }

//            [JsonProperty("title")]
//            public string CompanyName { get; set; }
//        }
//        private static readonly SemaphoreSlim batchSemaphore = new SemaphoreSlim(1, 1); // Semaphore to ensure only one batch executes at a time

//        private const int batchSizeLimit = 50; // Adjust this number based on SQL Server capacity

//        public static async Task ExecuteBatch(List<SqlCommand> batchedCommands)
//        {
//            var batchTimer = Stopwatch.StartNew();
//            const int maxRetries = 3;
//            int retryCount = 0;
//            bool success = false;

//            while (!success && retryCount < maxRetries)
//            {
//                try
//                {
//                    if (batchedCommands.Count > 0)
//                    {
//                        //Console.WriteLine($"[INFO] Attempting batch execution with {batchedCommands.Count} commands. Retry {retryCount + 1} of {maxRetries}");

//                        await batchSemaphore.WaitAsync();
//                        try
//                        {
//                            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//                            {
//                                await connection.OpenAsync();

//                                // Process in smaller chunks if necessary
//                                foreach (var batch in batchedCommands.Batch(batchSizeLimit))
//                                {
//                                    using (var transaction = connection.BeginTransaction())
//                                    {
//                                        try
//                                        {
//                                            foreach (var command in batch)
//                                            {
//                                                using (var clonedCommand = CloneCommand(command))
//                                                {
//                                                    clonedCommand.Connection = connection;
//                                                    clonedCommand.Transaction = transaction;
//                                                    await clonedCommand.ExecuteNonQueryAsync();
//                                                }
//                                            }

//                                            transaction.Commit();
//                                            //Console.WriteLine($"[INFO] Batch of {batch.Count()} commands executed successfully.");
//                                        }
//                                        catch (Exception ex)
//                                        {
//                                            transaction.Rollback();
//                                            //Console.WriteLine($"[ERROR] Transaction rolled back due to error: {ex.Message}");
//                                            throw;
//                                        }
//                                    }
//                                }

//                                batchedCommands.Clear();
//                                success = true;
//                            }
//                        }
//                        finally
//                        {
//                            batchSemaphore.Release();
//                        }
//                    }
//                    else
//                    {
//                        //Console.WriteLine("[INFO] No commands to execute.");
//                        success = true;
//                    }
//                }
//                catch (Exception ex)
//                {
//                    //Console.WriteLine($"Batch transaction failed. Error: {ex.Message}");
//                    retryCount++;
//                    if (retryCount >= maxRetries) throw;
//                }
//            }

//            batchTimer.Stop();
//            //Console.WriteLine($"[INFO] Batch execution time: {batchTimer.ElapsedMilliseconds} ms.");
//        }
//        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items, int batchSize)
//        {
//            return items.Select((item, inx) => new { item, inx })
//                        .GroupBy(x => x.inx / batchSize)
//                        .Select(g => g.Select(x => x.item));
//        }

//    }
//}
