using OpenQA.Selenium.Chrome;
using System.Data.SqlClient;
using Data;

namespace Nasdaq100FinancialScraper
{
    public class Program
    {
        public static readonly string connectionString = "Server=DESKTOP-SI08RN8\\SQLEXPRESS;Database=StockDataScraperDatabase;Integrated Security=True;";
        public static readonly SemaphoreSlim semaphore = new SemaphoreSlim(5, 5); // Allow 5 concurrent ChromeDriver instances
        static async Task Main(string[] args)
        {
            try
            {
                await ScrapeAndProcessDataAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("[INFO] Scraping completed.");
            }
        }
        public static int GetFiscalYear(DateTime endDate, int quarter, DateTime? fiscalYearEndDate = null)
        {
            if (quarter == 0 && fiscalYearEndDate.HasValue)
            {      // For annual reports, fiscal year is the year of the adjusted fiscal year end date
                return fiscalYearEndDate.Value.Year;
            }
            else
            {   // Existing logic for quarterly reports
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
                    default:
                        throw new ArgumentException("Invalid quarter value", nameof(quarter));
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
        public static async Task StoreCompanyDataInDatabase(List<Data.Data.CompanyInfo> companies)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                foreach (var company in companies)
                {
                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = @"
                            IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
                            BEGIN
                                INSERT INTO CompaniesList (CompanyName, CompanySymbol, CIK) 
                                VALUES (@CompanyName, @CompanySymbol, @CIK);
                            END";
                        command.Parameters.AddWithValue("@CompanyName", company.CompanyName);
                        command.Parameters.AddWithValue("@CompanySymbol", company.Ticker);
                        if (int.TryParse(company.CIK, out int cikValue))
                        {
                            command.Parameters.AddWithValue("@CIK", cikValue);
                        }
                        else
                        {
                            continue; // Skip this record if CIK is not valid
                        }
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        public static async Task<string> ScrapeReportsForCompany(string companySymbol, ChromeDriver driver)
{
    var localBatchedCommands = new List<SqlCommand>();
    var companyCIK = await StockScraperV3.URL.GetCompanyCIK(companySymbol);
    if (string.IsNullOrEmpty(companyCIK))
    {
        return $"Error: Unable to retrieve CIK for company symbol {companySymbol}.";
    }// Retrieve filing URLs for the last 10 years for both 10-K and 10-Q reports
    var filingUrls10K = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companyCIK, "10-K");
    var filingUrls10Q = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companyCIK, "10-Q");
    var combinedFilingUrls = filingUrls10K.Concat(filingUrls10Q).ToList();
    if (!combinedFilingUrls.Any())
    {
        return $"No filings found for {companySymbol}";
    }  // Prepare filings to process without filingType
    var filingsToProcess = combinedFilingUrls
        .Select(f => (f.url, f.description))
        .ToList();
    try
    {
        int companyId = await StockScraperV3.URL.GetCompanyIdBySymbol(companySymbol);
        if (companyId == 0)
        {
            return $"Error: Unable to retrieve Company ID for {companySymbol}.";
        }
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            using (SqlTransaction transaction = connection.BeginTransaction())
            {
                try
                {
                    string companyName = await GetCompanyNameBySymbol(companySymbol, connection, transaction);
                    var dataNonStatic = new DataNonStatic();
                    int someIntParameter = 0; // Assign appropriately based on your method's logic
                    await StockScraperV3.URL.ProcessFilings(driver, filingsToProcess, companyName, companySymbol, companyId, someIntParameter, dataNonStatic);
                    var completedEntries = await dataNonStatic.GetCompletedEntriesAsync(companyId);
                    if (completedEntries.Count > 0)
                    {
                        await dataNonStatic.SaveEntriesToDatabaseAsync(companyId, completedEntries);
                    }
                    if (localBatchedCommands.Any())
                    {
                        await Data.Data.ExecuteBatch(localBatchedCommands, connectionString);
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Transaction failed for {companySymbol}: {ex.Message}");
                    try
                    {
                        transaction.Rollback();
                        Console.WriteLine($"Transaction rolled back successfully for {companySymbol}.");
                    }
                    catch (Exception rollbackEx)
                    {
                        Console.WriteLine($"[ERROR] Rollback failed for {companySymbol}: {rollbackEx.Message}");
                    }
                    throw;
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Failed to process filings for {companySymbol}: {ex.Message}");
        throw;
    }
    int filingCount = filingsToProcess.Count;
    return $"Successfully scraped {filingCount} filings for {companySymbol}";
}
        private static async Task<string> GetCompanyNameBySymbol(string companySymbol, SqlConnection connection, SqlTransaction transaction)
        {
            string query = "SELECT CompanyName FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
            using (SqlCommand cmd = new SqlCommand(query, connection, transaction))
            {
                cmd.Parameters.AddWithValue("@CompanySymbol", companySymbol);
                var result = await cmd.ExecuteScalarAsync();
                if (result != null && result != DBNull.Value)
                {
                    return result.ToString();
                }
                else
                {
                    throw new Exception($"Company name not found for symbol {companySymbol}.");
                }
            }
        }
        private static async Task ScrapeAndProcessDataAsync()
        {
            try
            {
                var companies = await StockScraperV3.URL.GetNasdaq100CompaniesFromDatabase();
                var tasks = new List<Task>();
                var dataNonStatic = new DataNonStatic();
                foreach (var company in companies)
                {
                    await semaphore.WaitAsync(); // Wait until a slot is available
                    try
                    {        // Run scraper for each company
                        await StockScraperV3.URL.RunScraperAsync();
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            await connection.OpenAsync();
                            using (SqlTransaction transaction = connection.BeginTransaction())
                            {
                                try
                                {   // Use company.companyId to call CalculateAndSaveQ4InDatabase for the current company
                                    Data.Data.CalculateAndSaveQ4InDatabaseAsync(connection, transaction, company.companyId, dataNonStatic);
                                    transaction.Commit(); // Commit the transaction if successful
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[ERROR] Transaction failed for CompanyID: {company.companyId}: {ex.Message}");
                                    transaction.Rollback(); // Rollback the transaction on failure
                                }
                            }
                        }
                    }
                    finally
                    {
                        semaphore.Release(); // Release the semaphore
                    }
                }

                await Task.WhenAll(tasks); // Wait for all tasks to finish
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in ScrapeAndProcessDataAsync: {ex.Message}");
                throw; // Optionally, handle the exception or re-throw it as needed
            }
        }
    }
}

