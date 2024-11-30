// TODO: THE YEAR COLUMN DOESN'T FIT IN SYNC WITH THE FINANCIAL YEAR, WHICH IT NEEDS TO FOR THE CALCULATION OF THE MISSING QUARTER 4 AND NON-CUMULATIVE CASH FLOWS.
// MISSING DATA POINTS IN THE XBRL AND HTML DATA.
// so code

using DataElements;
using OpenQA.Selenium.Chrome;
using System.Data.SqlClient;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using StockScraperV3;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
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

        /// <summary>
        /// Calculates the fiscal year based on the end date and quarter, consistent with the rest of the program.
        /// </summary>
        public static int GetFiscalYear(DateTime endDate, int quarter, DateTime? fiscalYearEndDate = null)
        {
            if (quarter == 0 && fiscalYearEndDate.HasValue)
            {
                // For annual reports, fiscal year is the year of the adjusted fiscal year end date
                return fiscalYearEndDate.Value.Year;
            }
            else
            {
                // Existing logic for quarterly reports
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

        public static string GetColumnName(string elementName)
        {
            // Check if the element is part of the InstantDateElements or ElementsOfInterest sets (XBRL parsing)
            if (FinancialElementLists.InstantDateElements.Contains(elementName) || FinancialElementLists.ElementsOfInterest.Contains(elementName))
            {
                return elementName; // The element name itself is the column name in these cases
            }

            // Check if the element is part of HTMLElementsOfInterest (HTML parsing)
            foreach (var kvp in FinancialElementLists.HTMLElementsOfInterest)
            {
                // Check if the elementName matches any string in the key list
                if (kvp.Key.Contains(elementName))
                {
                    return kvp.Value.ColumnName; // Get the corresponding column name
                }
            }

            // Return empty string if no match is found
            return string.Empty;
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

        public static async Task<string> ScrapeReportsForCompany(string companySymbol)
        {
            
                // Initialize local batched commands (if necessary)
                var localBatchedCommands = new List<SqlCommand>();

                // Retrieve Company CIK (Central Index Key)
                var companyCIK = await StockScraperV3.URL.GetCompanyCIK(companySymbol);
                if (string.IsNullOrEmpty(companyCIK))
                {
                    return $"Error: Unable to retrieve CIK for company symbol {companySymbol}.";
                }

                // Retrieve filing URLs for the last 10 years for both 10-K and 10-Q reports
                var filingUrls10K = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companyCIK, "10-K");
                var filingUrls10Q = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companyCIK, "10-Q");

                // Combine both lists
                var combinedFilingUrls = filingUrls10K.Concat(filingUrls10Q).ToList();

                if (!combinedFilingUrls.Any())
                {
                    return $"No filings found for {companySymbol}";
                }

                // Prepare filings to process without filingType
                var filingsToProcess = combinedFilingUrls
                    .Select(f => (f.url, f.description))
                    .ToList(); // List<(string url, string description)>

                // Initialize a single ChromeDriver instance
                ChromeDriver driver = null;

                try
                {
                    // Initialize ChromeDriver session
                    driver = StockScraperV3.URL.StartNewSession();

                    // Retrieve company ID
                    int companyId = await StockScraperV3.URL.GetCompanyIdBySymbol(companySymbol);
                    if (companyId == 0)
                    {
                        return $"Error: Unable to retrieve Company ID for {companySymbol}.";
                    }

                    // Retrieve company name within the database transaction
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        await connection.OpenAsync();
                        using (SqlTransaction transaction = connection.BeginTransaction())
                        {
                            try
                            {
                                // Implement the missing method GetCompanyNameBySymbol
                                string companyName = await GetCompanyNameBySymbol(companySymbol, connection, transaction);

                                // Create a shared DataNonStatic instance
                                var dataNonStatic = new DataNonStatic();

                                // Define the missing int parameter
                                int someIntParameter = 0; // Assign appropriately based on your method's logic

                                // Process Filings within the transaction
                                await StockScraperV3.URL.ProcessFilings(
                                    driver,
                                    filingsToProcess,   // Correctly typed variable
                                    companyName,
                                    companySymbol,
                                    companyId,
                                    someIntParameter,   // Provide the missing int parameter
                                    dataNonStatic);     // Pass the shared DataNonStatic instance

                                // After all filings have been processed and data merged in dataNonStatic
                                var completedEntries = await dataNonStatic.GetCompletedEntriesAsync(companyId); // Corrected to async method

                                if (completedEntries.Count > 0) // No need for Count() if List<T>
                                {
                                    await dataNonStatic.SaveEntriesToDatabaseAsync(companyId, completedEntries);
                                    Console.WriteLine($"Successfully saved {completedEntries.Count} financial entries for {companyName} ({companySymbol}).");
                                }
                                else
                                {
                                    Console.WriteLine($"[INFO] No completed entries to save for {companyName} ({companySymbol}).");
                                }

                                // Execute any batched SQL commands within the transaction (if necessary)
                                if (localBatchedCommands.Any())
                                {
                                    await Data.Data.ExecuteBatch(localBatchedCommands, connectionString);
                                }

                                // Commit the transaction if all operations succeed
                                transaction.Commit();
                                Console.WriteLine($"Transaction committed successfully for {companyName} ({companySymbol}).");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ERROR] Transaction failed for {companySymbol}: {ex.Message}");
                                // Rollback the transaction on error
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
                    // Handle exceptions related to ChromeDriver and processing
                    Console.WriteLine($"[ERROR] Failed to process filings for {companySymbol}: {ex.Message}");
                    throw;
                }
                finally
                {
                    // Ensure the ChromeDriver is properly closed
                    driver?.Quit();
                }

                int filingCount = filingsToProcess.Count;
                return $"Successfully scraped {filingCount} filings for {companySymbol}";
            
}


        /// <summary>
        /// Retrieves the company name based on the company symbol.
        /// </summary>
        /// <param name="companySymbol">The stock symbol of the company.</param>
        /// <param name="connection">An open SQL connection.</param>
        /// <param name="transaction">The current SQL transaction.</param>
        /// <returns>The name of the company.</returns>
        /// <exception cref="Exception">Thrown if the company name cannot be found.</exception>
        private static async Task<string> GetCompanyNameBySymbol(string companySymbol, SqlConnection connection, SqlTransaction transaction)
        {
            string query = "SELECT CompanyName FROM Companies WHERE CompanySymbol = @CompanySymbol";
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


        private static async Task UpdateFinancialYear(int companyId, int year, int quarter, int financialYear, SqlConnection connection)
        {
            string updateQuery = @"
                UPDATE FinancialData
                SET FinancialYear = @FinancialYear
                WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

            using (SqlCommand command = new SqlCommand(updateQuery, connection))
            {
                command.Parameters.AddWithValue("@FinancialYear", financialYear);
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                command.Parameters.AddWithValue("@Quarter", quarter);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task ScrapeAndProcessDataAsync()
        {
            try
            {
                var companies = await StockScraperV3.URL.GetNasdaq100CompaniesFromDatabase();
                var tasks = new List<Task>();

                foreach (var company in companies)
                {
                    await semaphore.WaitAsync(); // Wait until a slot is available
                    try
                    {
                        // Run scraper for each company
                        await StockScraperV3.URL.RunScraperAsync();

                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            await connection.OpenAsync();
                            // Use company.companyId to call CalculateAndSaveQ4InDatabase for the current company
                            Data.Data.CalculateAndSaveQ4InDatabase(connection, company.companyId);
                        }
                    }
                    finally
                    {
                        semaphore.Release(); // Release the semaphore
                    }
                }qwerty

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


//using DataElements;
//using OpenQA.Selenium.Chrome;
//using System.Data.SqlClient;
//using System.Net.Http;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Xml.Linq;
//using StockScraperV3;


//namespace Nasdaq100FinancialScraper
//{
//    public class Program
//    {
//        public static readonly string connectionString = "Server=DESKTOP-SI08RN8\\SQLEXPRESS;Database=StockDataScraperDatabase;Integrated Security=True;";
//        public static List<SqlCommand> GetLocalBatchedCommands()
//        {
//            return new List<SqlCommand>(); // Create a new list for each task/thread
//        }
//        public static DateTime? globalStartDate = null;
//        public static DateTime? globalEndDate = null;
//        public static DateTime? globalInstantDate = null;
//        static async Task Main(string[] args)
//        {
//            try
//            {
//                Console.WriteLine("start");
//                await ScrapeAndProcessDataAsync();
//                // Add your scraping logic in this method
//            }
//            catch (Exception ex)
//            {
//                //Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
//            }
//            finally
//            {
//                //Console.WriteLine("[INFO] Calling CheckAndFillMissingFinancialYears for all companies.");
//                //await CheckAndFillMissingFinancialYears();
//            }
//        }

//        public static int CalculateFinancialYear(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            DateTime fiscalYearEnd = GetFiscalYearEndForSpecificYear(companyId, quarterEndDate.Year, connection, transaction);
//            DateTime fiscalYearStart = fiscalYearEnd.AddYears(-1).AddDays(1);

//            if (quarterEndDate >= fiscalYearStart && quarterEndDate <= fiscalYearEnd)
//            {
//                return fiscalYearEnd.Year;
//            }
//            else if (quarterEndDate < fiscalYearStart)
//            {
//                return fiscalYearEnd.Year - 1;
//            }
//            else
//            {
//                return fiscalYearEnd.Year + 1;
//            }
//        }
//        private static DateTime GetFiscalYearEndForSpecificYear(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//            SELECT TOP 1 EndDate
//            FROM FinancialData WITH (INDEX(IX_CompanyID_Year_Quarter))  -- Use index if available
//            WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 0
//            ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);

//                object result = command.ExecuteScalar();
//                if (result != null && DateTime.TryParse(result.ToString(), out DateTime fiscalYearEnd))
//                {
//                    return fiscalYearEnd;
//                }
//                else
//                {
//                    return new DateTime(year, 12, 31); // Default to end of the year
//                }
//            }
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
//        public static string GetColumnName(string elementName)
//        {
//            // Check if the element is part of the InstantDateElements or ElementsOfInterest sets (XBRL parsing)
//            if (FinancialElementLists.InstantDateElements.Contains(elementName) || FinancialElementLists.ElementsOfInterest.Contains(elementName))
//            {
//                return elementName; // The element name itself is the column name in these cases
//            }

//            // Check if the element is part of HTMLElementsOfInterest (HTML parsing)
//            foreach (var kvp in FinancialElementLists.HTMLElementsOfInterest)
//            {
//                // Check if the elementName matches any string in the key list
//                if (kvp.Key.Contains(elementName))
//                {
//                    return kvp.Value.ColumnName; // Get the corresponding column name
//                }
//            }

//            // Return empty string if no match is found
//            return string.Empty;
//        }
//        public static async Task StoreCompanyDataInDatabase(List<Data.Data.CompanyInfo> companies)
//        {
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                foreach (var company in companies)
//                {
//                    using (var command = new SqlCommand())
//                    {
//                        command.Connection = connection;
//                        command.CommandText = @"
//                            IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
//                            BEGIN
//                                INSERT INTO CompaniesList (CompanyName, CompanySymbol, CIK) 
//                                VALUES (@CompanyName, @CompanySymbol, @CIK);
//                            END";

//                        command.Parameters.AddWithValue("@CompanyName", company.CompanyName);
//                        command.Parameters.AddWithValue("@CompanySymbol", company.Ticker);
//                        if (int.TryParse(company.CIK, out int cikValue))
//                        {
//                            command.Parameters.AddWithValue("@CIK", cikValue);
//                        }
//                        else
//                        {
//                            continue; // Skip this record if CIK is not valid
//                        }

//                        await command.ExecuteNonQueryAsync();
//                    }
//                }
//            }
//        }
//        public static async Task<string> ScrapeReportsForCompany(string companySymbol)
//        {
//            try
//            {
//                // Initialize local batched commands
//                var localBatchedCommands = new List<SqlCommand>();

//                var companyCIK = await StockScraperV3.URL.GetCompanyCIK(companySymbol);

//                // Retrieve filing URLs
//                var filingUrls = await StockScraperV3.URL.GetFilingUrlsForLast10Years(companyCIK, "10-K");
//                filingUrls.AddRange(await StockScraperV3.URL.GetFilingUrlsForLast10Years(companyCIK, "10-Q"));

//                if (filingUrls.Any())
//                {
//                    // Use the global semaphore
//                    var semaphore = Program.semaphore;

//                    ChromeDriver driver = null;

//                    try
//                    {
//                        // Wait on the semaphore before starting
//                        await semaphore.WaitAsync();

//                        try
//                        {
//                            driver = StockScraperV3.URL.StartNewSession();
//                        }
//                        catch (Exception ex)
//                        {
//                            semaphore.Release();
//                            throw new Exception("Failed to create ChromeDriver", ex);
//                        }

//                        int companyId = await StockScraperV3.URL.GetCompanyIdBySymbol(companySymbol);
//                        int groupIndex = 0;

//                        // Pass the semaphore to ProcessFilings
//                        await StockScraperV3.URL.ProcessFilings(
//                            driver,
//                            filingUrls,
//                            companySymbol,
//                            companySymbol,
//                            companyId,
//                            groupIndex,
//                            localBatchedCommands,
//                            semaphore);

//                        // Execute batched commands after processing filings
//                        await Data.Data.ExecuteBatch(localBatchedCommands);
//                    }
//                    finally
//                    {
//                        driver?.Quit();
//                        semaphore.Release();
//                    }

//                    return $"Successfully scraped {filingUrls.Count} filings for {companySymbol}";
//                }

//                return $"No filings found for {companySymbol}";
//            }
//            catch (Exception ex)
//            {
//                return $"Error scraping company {companySymbol}: {ex.Message}";
//            }
//        }
//        private static int GetFinancialYearForQuarter(int quarter, DateTime endDate)
//        {
//            switch (quarter)
//            {
//                case 1:
//                    return endDate.AddMonths(9).Year;
//                case 2:
//                    return endDate.AddMonths(6).Year;
//                case 3:
//                    return endDate.AddMonths(3).Year;
//                case 4:
//                    return endDate.Year;
//                default:
//                    throw new InvalidOperationException("Invalid quarter value: " + quarter);
//            }
//        }
//        private static async Task UpdateFinancialYear(int companyId, int year, int quarter, int financialYear, SqlConnection connection)
//        {
//            string updateQuery = @"
//                UPDATE FinancialData
//                SET FinancialYear = @FinancialYear
//                WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//            using (SqlCommand command = new SqlCommand(updateQuery, connection))
//            {
//                command.Parameters.AddWithValue("@FinancialYear", financialYear);
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", quarter);

//                await command.ExecuteNonQueryAsync();
//            }
//        }
//        public static readonly SemaphoreSlim semaphore = new SemaphoreSlim(5, 5); // Allow 5 concurrent ChromeDriver instances

//        private static async Task ScrapeAndProcessDataAsync()
//        {
//            try
//            {
//                await Data.Data.ProcessUnfinishedRows();
//                var companies = await StockScraperV3.URL.GetNasdaq100CompaniesFromDatabase();
//                var tasks = new List<Task>();

//                foreach (var company in companies)
//                {
//                    await semaphore.WaitAsync(); // Wait until a slot is available
//                    try
//                    {
//                        // Pass the entire company object (or tuple) to RunScraperAsync
//                        await StockScraperV3.URL.RunScraperAsync(); // Pass company instead of company.symbol
//                    }
//                    finally
//                    {
//                        semaphore.Release(); // Release the semaphore
//                    }
//                }

//                await Task.WhenAll(tasks); // Wait for all tasks to finish
//            }
//            catch (Exception ex)
//            {
//                throw; // Handle exception as needed
//            }
//        }
//    }
//}