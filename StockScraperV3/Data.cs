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
        {  // Find an existing key with the same Fiscal Year and Quarter
            var existingEntryKey = FinancialEntries.Keys.FirstOrDefault(key =>
            {
                var parts = key.Split('_');
                if (parts.Length != 5) return false; // Ensure the key structure matches                
                if (!parts[1].StartsWith("FY") || !parts[2].StartsWith("Q"))// Extract Fiscal Year and Quarter from the key
                    return false; // Ensure the key contains Fiscal Year and Quarter                
                if (!int.TryParse(parts[1].Substring(2), out int existingFiscalYear))// Parse Fiscal Year
                    return false;                
                if (!int.TryParse(parts[2].Substring(1), out int existingQuarter))// Parse Quarter
                    return false;                
                return existingFiscalYear == entry.Year && existingQuarter == entry.Quarter;// Compare Fiscal Year and Quarter
            });
            if (existingEntryKey != null)
            {              // Merge with the existing entry
                FinancialEntries[existingEntryKey].MergeEntries(entry);
            }
            else
            {   // Add as a new entry
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
            int fiscalYear = Data.GetFiscalYear(entry.StandardEndDate, entry.Quarter, entry.FiscalYearEndDate);
            string key = $"{entry.CompanyID}_FY{fiscalYear}_Q{entry.Quarter}_{startDateStr}_{endDateStr}";
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
        public int Year { get; set; }
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
        public async Task SaveAllEntriesToDatabaseAsync(int companyId)
        {
            var companyData = await GetOrLoadCompanyFinancialDataAsync(companyId);
            var entriesToSave = companyData.GetCompletedEntries();

            if (!entriesToSave.Any())
            {
                return;
            }
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await Data.SaveCompleteEntryToDatabaseAsync(entriesToSave, connection, transaction);
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
                            await Data.SaveCompleteEntryToDatabaseAsync(mergedEntries, connection, transaction);
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
                            StartDate = reader.GetDateTime(1).Date, // Normalize to date-only
                            EndDate = reader.GetDateTime(2).Date,   // Normalize to date-only
                            Quarter = reader.GetInt32(3),
                            IsHtmlParsed = reader.GetBoolean(4),
                            IsXbrlParsed = reader.GetBoolean(5),
                            FinancialValues = Data.ParseFinancialDataJson(reader.GetString(6)),
                            FiscalYearEndDate = null // Initialized as null
                        };
                        if (entry.Quarter == 0) // Set FiscalYearEndDate for annual reports
                        {
                            entry.FiscalYearEndDate = entry.EndDate;
                        }
                        entries.Add(entry);
                    }
                }
            }
            return entries;
        }
        public async Task CalculateAndSaveQ4Async(int companyId)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await Data.CalculateAndSaveQ4InDatabaseAsync(connection, transaction, companyId, this);
                        transaction.Commit();
                        
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Failed to insert Q4 data for CompanyID: {companyId}: {ex.Message}");
                        try
                        {
                            transaction.Rollback();
                            Console.WriteLine($"[INFO] Transaction rolled back for Q4 data of CompanyID: {companyId}.");
                        }
                        catch (Exception rollbackEx)
                        {
                            Console.WriteLine($"[ERROR] Transaction rollback failed for Q4 data of CompanyID: {companyId}: {rollbackEx.Message}");
                        }
                        throw;
                    }
                }
            }
        }
        public async Task<List<FinancialDataEntry>> GetCompletedEntriesAsync(int companyId)
        {
            var companyData = await GetOrLoadCompanyFinancialDataAsync(companyId);
            return companyData.GetCompletedEntries();
        }
        public Task AddParsedDataAsync(int companyId, FinancialDataEntry parsedData)
        {
            // Just add to in-memory cache, no DB calls
            if (!companyFinancialDataCache.TryGetValue(companyId, out var companyData))
            {
                companyData = new CompanyFinancialData(companyId);
                companyFinancialDataCache[companyId] = companyData;
            }
            companyData.AddOrUpdateEntry(parsedData);
            return Task.CompletedTask;
        }
        public CompanyFinancialData GetOrCreateCompanyFinancialData(int companyId)
        {
            if (!companyFinancialDataCache.TryGetValue(companyId, out var companyData))
            {
                companyData = new CompanyFinancialData(companyId);
                companyFinancialDataCache[companyId] = companyData;
            }
            return companyData;
        }
        private ConcurrentDictionary<string, FinancialDataEntry> financialDataCache = new ConcurrentDictionary<string, FinancialDataEntry>();
    }
    public static class Data
    {
       public static async Task<(int fiscalYear, int quarter, DateTime fiscalYearEndDate)> DetermineFiscalYearAndQuarterAsync(int companyId, DateTime reportEndDate, DataNonStatic dataNonStatic)
    {
        try
        {// Step 1: Load company financial data
        var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(companyId);
        if (companyData == null)
        {
            throw new Exception($"Failed to load CompanyData for CompanyID: {companyId}.");
        }  // Step 2: Extract all annual reports (Quarter = 0)
        var annualReports = companyData.FinancialEntries.Values
            .Where(entry => entry.Quarter == 0)
            .OrderByDescending(entry => entry.EndDate)
            .ToList();
        if (annualReports == null || annualReports.Count == 0)
        {
            throw new Exception($"No annual reports found for CompanyID: {companyId}.");
        }        // Step 3: Find the most recent annual report before the quarterly report
        var relevantAnnualReport = annualReports
            .Where(report => report.EndDate.Date <= reportEndDate.Date)
            .OrderByDescending(report => report.EndDate)
            .FirstOrDefault();
        if (relevantAnnualReport == null)
        {   // Handle if no annual report is before the quarterly report
            relevantAnnualReport = annualReports.Last(); // Use the earliest annual report            
        }  // Step 4: Calculate Fiscal Year Start and End Dates
        DateTime fiscalYearEndDate = relevantAnnualReport.EndDate.Date;
        DateTime fiscalYearStartDate = fiscalYearEndDate.AddYears(-1).AddDays(1).Date;        
        int fiscalYear = fiscalYearEndDate.Year;// Step 5: Declare Fiscal Year Number before using it
                // Step 6: Validate that reportEndDate falls within the fiscal year
       if (reportEndDate.Date < fiscalYearStartDate || reportEndDate.Date > fiscalYearEndDate)
        {   // Adjust the fiscal year to the next year
            fiscalYearStartDate = fiscalYearEndDate.AddDays(1).Date;
            fiscalYearEndDate = fiscalYearStartDate.AddYears(1).AddDays(-1).Date;
            fiscalYear = fiscalYearEndDate.Year; // Reassign fiscalYear after adjustment
        }     // Step 7: Calculate the number of months between fiscalYearStartDate and reportEndDate
        int monthsDifference = ((reportEndDate.Year - fiscalYearStartDate.Year) * 12) + reportEndDate.Month - fiscalYearStartDate.Month;
        int determinedQuarter = 1; // Default value
        float rawdifference = monthsDifference / 3f; // Use float division
        if (monthsDifference > 1 && monthsDifference < 4.5f)
        {
            determinedQuarter = 1;
        }
        else if (monthsDifference > 4.5f && monthsDifference < 7.5f)
        {
            determinedQuarter = 2;
        }
        else if (monthsDifference > 7.5f && monthsDifference < 10.5f)
        {
            determinedQuarter = 3;
        }
        else
        {
            determinedQuarter = 4;
        }
        while (determinedQuarter > 4)
        {
            determinedQuarter -= 4;
        }

        while (determinedQuarter < 1)
        {
            determinedQuarter += 4;
        }
        if (determinedQuarter < 1 || determinedQuarter > 4)
        {
            throw new Exception($"Calculated quarter {determinedQuarter} is out of range for CompanyID: {companyId}.");
        }
        return (fiscalYear, determinedQuarter, fiscalYearEndDate);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Exception in DetermineFiscalYearAndQuarterAsync: {ex.Message}");
        throw; // Re-throw the exception after logging
    }
}
        public static DateTime AdjustToNearestQuarterEndDate(DateTime fiscalYearEndDate)
        {      // Define standard quarter end months: March, June, September, December
            var quarterEndMonths = new List<int> { 3, 6, 9, 12 };            
            int originalMonth = fiscalYearEndDate.Month;// Find the nearest quarter end month to the original fiscal year end month
            int nearestQuarterEndMonth = quarterEndMonths
                .OrderBy(m => Math.Abs(m - originalMonth))
                .First();
            int lastDay = DateTime.DaysInMonth(fiscalYearEndDate.Year, nearestQuarterEndMonth);
            DateTime adjustedDate = new DateTime(fiscalYearEndDate.Year, nearestQuarterEndMonth, lastDay);
            return adjustedDate;
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
                case 0:  // For annual reports, the entire calendar year
                    periodStart = new DateTime(fiscalYear, 1, 1);
                    periodEnd = new DateTime(fiscalYear, 12, 31);
                    break;
                default:
                    throw new ArgumentException("Invalid quarter value", nameof(quarter));
            }
            return (periodStart, periodEnd);
        }
        private static async Task<DateTime> GetQuarterEndDateAsync(SqlConnection connection, SqlTransaction transaction, int companyId, int year, int quarter)
        {
            string query = @"
SELECT EndDate
FROM FinancialData
WHERE CompanyID = @CompanyID 
  AND YEAR(EndDate) = @Year 
  AND Quarter = @Quarter"; // Ensure 'Quarter' column exists

            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {    // Add parameters with correct names
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                command.Parameters.AddWithValue("@Quarter", quarter);
                try
                {
                    object result = await command.ExecuteScalarAsync();

                    if (result != null && DateTime.TryParse(result.ToString(), out DateTime endDate))
                    {
                        return endDate;
                    }
                    else
                    {
                        return DateTime.MinValue;
                    }
                }
                catch (SqlException sqlEx)
                {
                    Console.WriteLine($"[SQL ERROR] GetQuarterEndDateAsync: {sqlEx.Message}");
                    throw; // Re-throw to handle upstream
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] GetQuarterEndDateAsync: {ex.Message}");
                    throw; // Re-throw to handle upstream
                }
            }
        }
        private static List<string> GetAllFinancialElementsFromEntries(List<FinancialDataEntry> financialEntries, int year)
        {
            var elementNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var entriesForYear = financialEntries.Where(entry => entry.Year == year);
            foreach (var entry in entriesForYear)
            {
                foreach (var key in entry.FinancialValues.Keys)
                {
                    elementNames.Add(key);
                }
            }
            return elementNames.ToList();
        }
        private static async Task<List<string>> GetAllFinancialElementsAsync(SqlConnection connection, SqlTransaction transaction, int companyId, int year)
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
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
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
        private static async Task ProcessAllFinancialElementsAsync(int companyId, int year, Dictionary<string, object> q4Values, List<string> elementNames, List<FinancialDataEntry> financialEntries)
        {   // Group financial entries by Quarter for quick access
            var entriesByQuarter = financialEntries
                .Where(entry => entry.Year == year)
                .GroupBy(entry => entry.Quarter)
                .ToDictionary(g => g.Key, g => g.FirstOrDefault());// Use a thread-safe dictionary to store Q4 values when processing in parallel
            var q4ValuesConcurrent = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);            
            Parallel.ForEach(elementNames, elementName =>// Parallelize the processing of financial elements
            {   // Adjust the element name for annual value retrieval
                string annualElementName = AdjustElementNameForAnnual(elementName);
                string q1ElementName = AdjustElementNameForQuarter(elementName, 1);
                string q2ElementName = AdjustElementNameForQuarter(elementName, 2);
                string q3ElementName = AdjustElementNameForQuarter(elementName, 3);   // Retrieve financial values from in-memory data
                decimal? annualValue = GetFinancialValueFromEntries(entriesByQuarter, 0, annualElementName);
                decimal? q1Value = GetFinancialValueFromEntries(entriesByQuarter, 1, q1ElementName);
                decimal? q2Value = GetFinancialValueFromEntries(entriesByQuarter, 2, q2ElementName);
                decimal? q3Value = GetFinancialValueFromEntries(entriesByQuarter, 3, q3ElementName);
                decimal? q4Value;
                if (annualValue.HasValue)
                {
                    bool isHtmlElement = elementName.StartsWith("HTML_", StringComparison.OrdinalIgnoreCase);
                    string statementType = GetStatementType(elementName);
                    bool isBalanceSheet = statementType.Equals("Balance Sheet", StringComparison.OrdinalIgnoreCase);
                    if (isBalanceSheet && !statementType.Equals("Other", StringComparison.OrdinalIgnoreCase))
                    {
                        // For Balance Sheet items, Q4 = Annual value
                        q4Value = annualValue.Value;
                    }
                    else
                    {
                        // For other statements, Q4 = Annual - (Q1 + Q2 + Q3)
                        q4Value = annualValue.Value - (q1Value.GetValueOrDefault() + q2Value.GetValueOrDefault() + q3Value.GetValueOrDefault());
                    }
                    string adjustedElementName = AdjustElementNameForQ4(elementName);
                    q4ValuesConcurrent[adjustedElementName] = q4Value; // Assign the value with the adjusted key
                }
            });     // Merge the concurrent dictionary into the main q4Values dictionary
            foreach (var kvp in q4ValuesConcurrent)
            {
                q4Values[kvp.Key] = kvp.Value;
            }
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
        public static (DateTime periodStart, DateTime periodEnd) GetStandardPeriodDates(int fiscalYear, int quarter, DateTime? PeriodEndDate = null)
{
    if (PeriodEndDate.HasValue)
    {
        DateTime fiscalYearStart = PeriodEndDate.Value.AddYears(-1).AddDays(1);
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
                periodEnd = PeriodEndDate.Value;
                break;
            case 0: // For annual reports, the entire fiscal year
                periodStart = fiscalYearStart;
                periodEnd = PeriodEndDate.Value;
                break;
            default:
                throw new ArgumentException("Invalid quarter value", nameof(quarter));
        }
        return (periodStart, periodEnd);
    }
    else
    {    // Fallback logic based on calendar year
        var dates = GetStandardPeriodDatesBasedOnCalendarYear(fiscalYear, quarter);
        return dates;
    }
}
        private static decimal? GetFinancialValueFromEntries(Dictionary<int, FinancialDataEntry> entriesByQuarter, int quarter, string elementName)
        {
            if (entriesByQuarter.TryGetValue(quarter, out var entry))
            {
                if (entry.FinancialValues.TryGetValue(elementName, out var valueObj))
                {
                    if (decimal.TryParse(valueObj.ToString(), out decimal value))
                    {
                        return value;
                    }
                }
            }
            return null;
        }
        private static async Task<decimal?> GetFinancialValueAsync(SqlConnection connection, SqlTransaction transaction, int companyId, int year, int quarter, string columnName)
        {
            string query = @"
SELECT FinancialDataJson
FROM FinancialData
WHERE CompanyID = @CompanyID 
  AND Quarter = @Quarter
  AND EndDate >= @FiscalYearStart 
  AND EndDate <= @FiscalYearEnd";   // Asynchronously get fiscal year start and end dates
            (DateTime fiscalYearStart, DateTime fiscalYearEnd) = await GetFiscalYearStartEndAsync(companyId, year, connection, transaction);
            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Quarter", quarter);
                command.Parameters.AddWithValue("@FiscalYearStart", fiscalYearStart);
                command.Parameters.AddWithValue("@FiscalYearEnd", fiscalYearEnd);
                try
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
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
                catch (SqlException sqlEx)
                {
                    Console.WriteLine($"[SQL ERROR] GetFinancialValueAsync: {sqlEx.Message}");
                    throw; // Re-throw to handle upstream
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] GetFinancialValueAsync: {ex.Message}");
                    throw; // Re-throw to handle upstream
                }
            }
            return null; // Return null if value is missing
        }
        private static async Task<(DateTime fiscalYearStart, DateTime fiscalYearEnd)> GetFiscalYearStartEndAsync(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
        {
            string query = @"
SELECT StartDate, EndDate
FROM FinancialData
WHERE CompanyID = @CompanyID 
  AND YEAR(EndDate) = @Year 
  AND Quarter = 0"; // Assuming Quarter = 0 denotes annual reports

            using (SqlCommand command = new SqlCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                try
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            DateTime fiscalYearStart = reader.GetDateTime(0).Date; // Normalize to date-only
                            DateTime fiscalYearEnd = reader.GetDateTime(1).Date;   // Normalize to date-only
                            return (fiscalYearStart, fiscalYearEnd);
                        }
                        else
                        {      // Handle the case where no annual report is found
                            Console.WriteLine($"[WARNING] No annual report found for CompanyID: {companyId}, Year: {year}. Defaulting to calendar year.");
                            DateTime defaultStart = new DateTime(year, 1, 1); // Already date-only
                            DateTime defaultEnd = new DateTime(year, 12, 31); // Already date-only
                            return (defaultStart, defaultEnd);
                        }
                    }
                }
                catch (SqlException sqlEx)
                {
                    Console.WriteLine($"[SQL ERROR] GetFiscalYearStartEndAsync: {sqlEx.Message}");
                    throw; // Re-throw to handle upstream
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] GetFiscalYearStartEndAsync: {ex.Message}");
                    throw; // Re-throw to handle upstream
                }
            }
        }      
        private static async Task<decimal?> GetNullableFinancialValueAsync(SqlConnection connection, SqlTransaction transaction, int companyId, int year, int quarter, string elementName)
        {    // Reuse GetFinancialValueAsync as they perform the same operation
            return await GetFinancialValueAsync(connection, transaction, companyId, year, quarter, elementName);
        }
        private static async Task InsertQ4DataAsync(SqlConnection connection, SqlTransaction transaction, int companyId, int year, DateTime startDate, DateTime endDate, Dictionary<string, object> q4Values)
        { // Calculate fiscal year based on endDate and quarter
            int fiscalYear = CompanyFinancialData.GetFiscalYear(endDate, 4, (DateTime?)endDate);
            (DateTime standardStartDate, DateTime standardEndDate) = Data.GetStandardPeriodDates(fiscalYear, 4, (DateTime?)endDate);
            var q4Entry = new FinancialDataEntry
            {
                CompanyID = companyId,
                StartDate = startDate, // Parsed StartDate
                EndDate = endDate,     // Parsed EndDate
                Quarter = 4,
                Year = fiscalYear,     // **Explicitly set Year**
                IsHtmlParsed = true,
                IsXbrlParsed = true,
                FinancialValues = q4Values
            };
            var entries = new List<FinancialDataEntry> { q4Entry };
            await SaveCompleteEntryToDatabaseAsync(entries, connection, transaction);
        }
        public static async Task CalculateAndSaveQ4InDatabaseAsync(SqlConnection connection, SqlTransaction transaction, int companyId, DataNonStatic dataNonStatic)
        {
            var companyData = await dataNonStatic.GetOrLoadCompanyFinancialDataAsync(companyId);
            if (companyData == null)
            {
                Console.WriteLine($"[ERROR] Failed to load CompanyData for CompanyID: {companyId}. Cannot calculate Q4.");
                return;
            }
            DateTime? fiscalYearEndDate = companyData.GetMostRecentFiscalYearEndDate();
            if (!fiscalYearEndDate.HasValue)
            {
                Console.WriteLine($"[ERROR] No fiscal year end date found for CompanyID: {companyId}. Cannot calculate Q4.");
                return;
            }
            DateTime stopDate = new DateTime(2012, 1, 1);
            DateTime currentFiscalYearEndDate = fiscalYearEndDate.Value;
            int currentFiscalYear = CompanyFinancialData.GetFiscalYear(currentFiscalYearEndDate, 0, currentFiscalYearEndDate);
            int maxIterations = 11;
            int iteration = 0;
            List<FinancialDataEntry> q4Entries = new List<FinancialDataEntry>();// Load all financial entries into memory
            var financialEntries = companyData.FinancialEntries.Values.ToList();
            while (currentFiscalYearEndDate >= stopDate && iteration < maxIterations)
            {              
                (DateTime fiscalYearStartDate, DateTime fiscalYearEndDateActual) = Data.GetStandardPeriodDates(currentFiscalYear, 0, currentFiscalYearEndDate);
                DateTime q3EndDate = GetQuarterEndDateFromEntries(financialEntries, companyId, currentFiscalYear, 3);
                DateTime q4StartDate;
                if (q3EndDate != DateTime.MinValue)
                {
                    q4StartDate = q3EndDate.AddDays(1);
                }
                else
                {
                    TimeSpan fiscalYearDuration = fiscalYearEndDateActual - fiscalYearStartDate;
                    int daysInFiscalYear = fiscalYearDuration.Days + 1;
                    int daysPerQuarter = daysInFiscalYear / 4;
                    q4StartDate = fiscalYearStartDate.AddDays(3 * daysPerQuarter);
                }
                DateTime q4EndDate = fiscalYearEndDateActual;
                var q4Values = new Dictionary<string, object>();
                var allElementNames = GetAllFinancialElementsFromEntries(financialEntries, currentFiscalYear);
                await ProcessAllFinancialElementsAsync(companyId, currentFiscalYear, q4Values, allElementNames, financialEntries);
                try
                {
                    var q4Entry = new FinancialDataEntry
                    {
                        CompanyID = companyId,
                        StartDate = q4StartDate,
                        EndDate = q4EndDate,
                        Quarter = 4,
                        Year = currentFiscalYear,
                        IsHtmlParsed = true,
                        IsXbrlParsed = true,
                        FinancialValues = q4Values
                    };
                    q4Entries.Add(q4Entry);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Failed to prepare Q4 data for Fiscal Year {currentFiscalYear}: {ex.Message}");
                }
                currentFiscalYearEndDate = currentFiscalYearEndDate.AddYears(-1);
                currentFiscalYear = CompanyFinancialData.GetFiscalYear(currentFiscalYearEndDate, 0, currentFiscalYearEndDate);
                iteration++;
            }
            if (q4Entries.Count > 0)
            {
                try
                {
                    await SaveCompleteEntryToDatabaseAsync(q4Entries, connection, transaction);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Failed to insert Q4 data: {ex.Message}");
                }
            }            
        }
        private static DateTime GetQuarterEndDateFromEntries(List<FinancialDataEntry> financialEntries, int companyId, int year, int quarter)
        {
            var entry = financialEntries.FirstOrDefault(e =>
                e.CompanyID == companyId &&
                e.Year == year &&
                e.Quarter == quarter);
            return entry?.EndDate ?? DateTime.MinValue;
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
        public static async Task SaveCompleteEntryToDatabaseAsync(List<FinancialDataEntry> entries, SqlConnection connection, SqlTransaction transaction)
        {
            if (entries == null || !entries.Any()) return;   // Create a DataTable from entries
            DataTable table = CreateDataTable(entries);
            if (table.Rows.Count == 0)
            {
                return;
            } // Extract entries to check for existing records
            var entriesToCheck = entries.Select(e => new FinancialDataEntry
            {
                CompanyID = e.CompanyID,
                StartDate = e.StartDate,
                EndDate = e.EndDate,
                Quarter = e.Quarter
            }).ToList();  // Fetch existing records using exact matches
            DataTable existingRows = await FetchExistingRowsAsync(connection, transaction, entries.First().CompanyID, entriesToCheck);
            // Create a hash set of existing keys for quick lookup
            var existingKeys = new HashSet<(int CompanyID, DateTime StartDate, DateTime EndDate, int Quarter)>(
                existingRows.AsEnumerable().Select(row => (
                    row.Field<int>("CompanyID"),
                    row.Field<DateTime>("StartDate").Date,
                    row.Field<DateTime>("EndDate").Date,
                    row.Field<int>("Quarter")
                ))
            );  // Determine which entries are new and need to be inserted
            var rowsToInsertEnumerable = entries.Where(e =>!existingKeys.Contains((e.CompanyID, e.StartDate.Date, e.EndDate.Date, e.Quarter))).ToList();
            if (rowsToInsertEnumerable.Any())
            {
                DataTable rowsToInsert = CreateDataTable(rowsToInsertEnumerable);
                try// Perform bulk insert for new rows
                {
                    await BulkInsert(rowsToInsert, connection, transaction);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Bulk insert failed: {ex.Message}");
                    throw;
                }
            }// Update existing records if necessary
            await UpdateExistingRecords(entries, connection, transaction, existingRows, TimeSpan.FromDays(15));
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
                {    // Retrieve existing FinancialDataJson from the database
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
                        var existingFinancialValues = ParseFinancialDataJson(existingJson);
                        var mergedFinancialValues = new Dictionary<string, object>(existingFinancialValues, StringComparer.OrdinalIgnoreCase);
                        foreach (var kvp in entry.FinancialValues)
                        {
                            mergedFinancialValues[kvp.Key] = kvp.Value;
                        } // Update the IsHtmlParsed and IsXbrlParsed flags
                        bool updatedIsHtmlParsed = existingIsHtmlParsed || entry.IsHtmlParsed;
                        bool updatedIsXbrlParsed = existingIsXbrlParsed || entry.IsXbrlParsed;   // Prepare update command
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
        public static async Task<DataTable> FetchExistingRowsAsync(SqlConnection connection, SqlTransaction transaction, int companyId, List<FinancialDataEntry> newEntries)
{
    var existingRows = new DataTable();
    if (newEntries == null || !newEntries.Any())
        return existingRows;    
    var keyTable = new DataTable();// Create a DataTable matching the TVP structure with Quarter
            keyTable.Columns.Add("CompanyID", typeof(int));
    keyTable.Columns.Add("StartDate", typeof(DateTime));
    keyTable.Columns.Add("EndDate", typeof(DateTime));
    keyTable.Columns.Add("Quarter", typeof(int)); // Include Quarter
    foreach (var entry in newEntries)
    {
        keyTable.Rows.Add(entry.CompanyID, entry.StartDate.Date, entry.EndDate.Date, entry.Quarter);
    }
    string query = @"
        SELECT fd.CompanyID, fd.StartDate, fd.EndDate, fd.Quarter
        FROM FinancialData fd
        INNER JOIN @Keys k
            ON fd.CompanyID = k.CompanyID
            AND fd.StartDate = k.StartDate
            AND fd.EndDate = k.EndDate
            AND fd.Quarter = k.Quarter";
    using (SqlCommand command = new SqlCommand(query, connection, transaction))
    {
        var tvpParam = command.Parameters.AddWithValue("@Keys", keyTable);
        tvpParam.SqlDbType = SqlDbType.Structured;
        tvpParam.TypeName = "dbo.FinancialDataKeyType"; // Ensure your TVP includes Quarter

        using (SqlDataAdapter adapter = new SqlDataAdapter(command))
        {
            await Task.Run(() => adapter.Fill(existingRows)); // Ensure asynchronous operation
        }
    }
    return existingRows;
}
        public static DataTable CreateDataTable(List<FinancialDataEntry> entries)
        {
            DataTable table = new DataTable();
            table.Columns.Add("CompanyID", typeof(int));
            table.Columns.Add("StartDate", typeof(DateTime));
            table.Columns.Add("EndDate", typeof(DateTime));
            table.Columns.Add("Quarter", typeof(int));
            table.Columns.Add("Year", typeof(int));
            table.Columns.Add("FinancialDataJson", typeof(string));
            table.Columns.Add("IsHtmlParsed", typeof(bool));
            table.Columns.Add("IsXbrlParsed", typeof(bool));
            var uniqueRows = new HashSet<(int CompanyID, DateTime StartDate, DateTime EndDate, int Quarter)>();
            foreach (var entry in entries)
            {       // Validate dates
                if (entry.StartDate < new DateTime(1753, 1, 1) || entry.EndDate < new DateTime(1753, 1, 1))
                {
                    Console.WriteLine($"[WARNING] Skipping entry with invalid dates for CompanyID: {entry.CompanyID}");
                    continue;
                }
                var uniqueKey = (entry.CompanyID, entry.StartDate.Date, entry.EndDate.Date, entry.Quarter);
                if (uniqueRows.Contains(uniqueKey))
                {
                    Console.WriteLine($"[INFO] Duplicate entry found for CompanyID: {entry.CompanyID}, StartDate: {entry.StartDate.Date}, EndDate: {entry.EndDate.Date}, Quarter: {entry.Quarter}. Skipping.");
                    continue; // Skip duplicates
                }
                uniqueRows.Add(uniqueKey);
                DataRow row = table.NewRow();
                row["CompanyID"] = entry.CompanyID;
                row["StartDate"] = entry.StartDate.Date; // Normalize to date-only
                row["EndDate"] = entry.EndDate.Date;     // Normalize to date-only
                row["Quarter"] = entry.Quarter;
                row["Year"] = entry.Year;
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
        public static int CalculateQuarterByFiscalDayMonth(DateTime reportDate, DateTime fiscalYearEndDate)
        {
            DateTime fiscalYearStart = fiscalYearEndDate.AddYears(-1).AddDays(1);            
            var quarters = new List<(DateTime Start, DateTime End)>// Define standard fiscal quarters
    {
        (fiscalYearStart, fiscalYearStart.AddMonths(3).AddDays(-1)), // Q1
        (fiscalYearStart.AddMonths(3), fiscalYearStart.AddMonths(6).AddDays(-1)), // Q2
        (fiscalYearStart.AddMonths(6), fiscalYearStart.AddMonths(9).AddDays(-1)), // Q3
        (fiscalYearStart.AddMonths(9), fiscalYearEndDate) // Q4
    };
            for (int i = 0; i < quarters.Count; i++)
            {
                if (reportDate >= quarters[i].Start && reportDate <= quarters[i].End)
                    return i + 1; // Quarters are 1-indexed
            }  // If reportDate is outside fiscal year range, determine based on proximity
            if (reportDate < fiscalYearStart)
                return 0; // Before fiscal year start
            else
                return 4; // After fiscal year end
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
