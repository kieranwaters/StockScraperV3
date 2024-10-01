
// TODO: THE YEAR COLOUMN DOESNT FIT IN SYNC WITH THE FINANCIAL YEAR, WHICH IT NEED TO FOR THE CALCULATION OF THE MISING QUARTER 4 AND NON CUMULATIVE CASH FLOWS.
// MISSING DATA POINTS IN THE XBRL AND HTML DATA.

using DataElements;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Nasdaq100FinancialScraper
{
    class Program
    {
        public static readonly string connectionString = "Server=LAPTOP-871MLHAT\\sqlexpress;Database=StockDataScraperDatabase;Integrated Security=True;";
        public static List<SqlCommand> batchedCommands = new List<SqlCommand>();
        public static DateTime? globalStartDate = null;
        public static DateTime? globalEndDate = null;
        public static DateTime? globalInstantDate = null;
        public static readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("[INFO] Starting the program.");
                await ScrapeAndProcessDataAsync();  // Add your scraping logic in this method
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("[INFO] Calling CheckAndFillMissingFinancialYears for all companies.");
                await CheckAndFillMissingFinancialYears();
            }
        }

        private static int CalculateFinancialYear(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
        {
            DateTime fiscalYearEnd = GetFiscalYearEndForSpecificYear(companyId, quarterEndDate.Year, connection, transaction);
            DateTime fiscalYearStart = fiscalYearEnd.AddYears(-1).AddDays(1);

            if (quarterEndDate >= fiscalYearStart && quarterEndDate <= fiscalYearEnd)
            {
                return fiscalYearEnd.Year;
            }
            else if (quarterEndDate < fiscalYearStart)
            {
                return fiscalYearEnd.Year - 1;
            }
            else
            {
                return fiscalYearEnd.Year + 1;
            }
        }
        private static DateTime GetFiscalYearEndForSpecificYear(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandText = @"
                            SELECT TOP 1 EndDate
                            FROM FinancialData
                            WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 0
                            ORDER BY EndDate DESC";

                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);

                object result = command.ExecuteScalar();
                if (result != null && DateTime.TryParse(result.ToString(), out DateTime fiscalYearEnd))
                {
                    return fiscalYearEnd;
                }
                else
                {
                    return new DateTime(year, 12, 31); // Default to end of the year
                }
            }
        }
        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, SqlConnection connection, SqlTransaction transaction)
        {
            try
            {
                using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
                {
                    // Retrieve the CompanyName and CompanySymbol dynamically from the CompaniesList table
                    command.CommandText = @"
                SELECT CompanyName, CompanySymbol
                FROM CompaniesList
                WHERE CompanyID = @CompanyID";
                    command.Parameters.AddWithValue("@CompanyID", companyId);

                    using (var reader = command.ExecuteReader())
                    {
                        if (!reader.Read())
                        {
                            Console.WriteLine($"[ERROR] No matching company found for CompanyID: {companyId}");
                            return;
                        }

                        string companyName = reader["CompanyName"].ToString();
                        string companySymbol = reader["CompanySymbol"].ToString();
                        reader.Close();

                        // Log the company details
                        Console.WriteLine($"[INFO] Processing data for {companyName} ({companySymbol})");
                    }

                    // Check if the report is within the date range for parsing
                    

                    // Calculate the financial year directly from the endDate for both quarterly and annual reports
                    int financialYear = (quarter == 0) ? endDate.Year : GetFinancialYearForQuarter(quarter, endDate);

                    // Log if the financial year couldn't be determined
                    if (financialYear == 0)
                    {
                        Console.WriteLine($"[WARNING] Could not determine financial year for CompanyID: {companyId}, Quarter: {quarter}. Setting to 0.");
                    }

                    // Fetch or create the PeriodID from the Periods table
                    command.CommandText = @"
                DECLARE @ExistingPeriodID INT;
                SET @ExistingPeriodID = (SELECT TOP 1 PeriodID FROM Periods WHERE Year = @Year AND Quarter = @Quarter);
                IF @ExistingPeriodID IS NOT NULL
                BEGIN
                    SELECT @ExistingPeriodID;
                END
                ELSE
                BEGIN
                    INSERT INTO Periods (Year, Quarter) VALUES (@Year, @Quarter);
                    SELECT SCOPE_IDENTITY();
                END";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@Year", financialYear);
                    command.Parameters.AddWithValue("@Quarter", quarter);

                    int periodId = Convert.ToInt32(command.ExecuteScalar());

                    // Calculate the start date based on the quarter (annual or quarterly)
                    DateTime startDate = quarter == 0 ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);

                    // Prepare insert or update command for FinancialYear
                    command.CommandText = @"
                IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
                BEGIN
                    UPDATE FinancialData 
                    SET FinancialYear = @FinancialYear, StartDate = @StartDate, EndDate = @EndDate
                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
                END
                ELSE
                BEGIN
                    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
                    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate);
                END";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    command.Parameters.AddWithValue("@PeriodID", periodId);
                    command.Parameters.AddWithValue("@FinancialYear", financialYear);
                    command.Parameters.AddWithValue("@Year", endDate.Year);
                    command.Parameters.AddWithValue("@Quarter", quarter);
                    command.Parameters.AddWithValue("@StartDate", startDate);
                    command.Parameters.AddWithValue("@EndDate", endDate);

                    // Execute the command for financial year
                    int rowsAffected = command.ExecuteNonQuery();
                    Console.WriteLine($"[INFO] {rowsAffected} row(s) affected for CompanyID: {companyId}, Quarter: {quarter}, FinancialYear: {financialYear}");

                    // Get the column name for the specific element
                    string columnName = GetColumnName(elementName);

                    // If the element does not map to a column, log and return
                    if (string.IsNullOrEmpty(columnName))
                    {
                        Console.WriteLine($"[WARNING] No matching column found for element name: {elementName}");
                        return;
                    }

                    // Prepare insert or update command for the element
                    command.CommandText = $@"
                IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
                BEGIN
                    UPDATE FinancialData 
                    SET [{columnName}] = @Value
                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
                END
                ELSE
                BEGIN
                    INSERT INTO FinancialData (CompanyID, PeriodID, Year, Quarter, StartDate, EndDate, [{columnName}], FinancialYear)
                    VALUES (@CompanyID, @PeriodID, @Year, @Quarter, @StartDate, @EndDate, @Value, @FinancialYear);
                END";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    command.Parameters.AddWithValue("@PeriodID", periodId);
                    command.Parameters.AddWithValue("@Year", endDate.Year);
                    command.Parameters.AddWithValue("@Quarter", quarter);
                    command.Parameters.AddWithValue("@StartDate", startDate);
                    command.Parameters.AddWithValue("@EndDate", endDate);
                    command.Parameters.AddWithValue("@Value", value);
                    command.Parameters.AddWithValue("@FinancialYear", financialYear);

                    // Execute the command for the element
                    int elementRowsAffected = command.ExecuteNonQuery();
                    Console.WriteLine($"[INFO] {elementRowsAffected} row(s) affected for CompanyID: {companyId}, Quarter: {quarter}, Element: {elementName}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving data for CompanyID: {companyId}, Element: {elementName}. Exception: {ex.Message}");
            }
        }


        // Helper method to get the column name based on the element name
        private static string GetColumnName(string elementName)
        {
            // Check if the element is part of the InstantDateElements or ElementsOfInterest sets (XBRL parsing)
            if (FinancialElementLists.InstantDateElements.Contains(elementName) || FinancialElementLists.ElementsOfInterest.Contains(elementName))
            {
                return elementName; // The element name itself is the column name in these cases
            }

            // Check if the element is part of HTMLElementsOfInterest (HTML parsing)
            if (FinancialElementLists.HTMLElementsOfInterest.TryGetValue(elementName, out var htmlElement))
            {
                return htmlElement.ColumnName; // Get the corresponding column name from the HTML elements dictionary
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
                            Console.WriteLine($"Invalid CIK value for {company.CompanyName} ({company.Ticker}): {company.CIK}");
                            continue; // Skip this record if CIK is not valid
                        }

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        private static async Task CheckAndFillMissingFinancialYears()
        {
            Console.WriteLine("[INFO] Entered CheckAndFillMissingFinancialYears for all companies.");

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    Console.WriteLine("[INFO] Connection to database opened.");

                    string query = @"
                        SELECT CompanyID, Year, Quarter, EndDate
                        FROM FinancialData
                        WHERE FinancialYear IS NULL OR FinancialYear = 0";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int companyId = reader.GetInt32(0);
                            int year = reader.GetInt32(1);
                            int quarter = reader.GetInt32(2);
                            DateTime endDate = reader.GetDateTime(3);

                            Console.WriteLine($"[INFO] Processing row: CompanyID={companyId}, Year={year}, Quarter={quarter}, EndDate={endDate}");

                            int financialYear = GetFinancialYearForQuarter(quarter, endDate);

                            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears: {ex.Message}");
            }
        }

        private static int GetFinancialYearForQuarter(int quarter, DateTime endDate)
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
                    return endDate.Year;
                default:
                    throw new InvalidOperationException("Invalid quarter value: " + quarter);
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
                Console.WriteLine($"[INFO] Updated FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter} to {financialYear}");
            }
        }
        private static async Task ScrapeAndProcessDataAsync()
        {
            try
            {
                Console.WriteLine("[INFO] Starting scraping and processing.");

                // Process any unfinished rows before starting
                await Data.Data.ProcessUnfinishedRows();
                Console.WriteLine("[INFO] ProcessUnfinishedRows completed.");

                // Get the list of companies from the database
                var companies = await StockScraperV3.URL.GetNasdaq100CompaniesFromDatabase();
                Console.WriteLine($"[INFO] Number of companies to process: {companies.Count}");

                // Loop through each company and process their reports individually
                foreach (var company in companies)
                {
                    Console.WriteLine($"[INFO] Starting processing for {company.companyName} ({company.symbol})");

                    // Run the scraper for this company and process filings
                    await StockScraperV3.URL.RunScraperAsync(); // Ensure this runs for each company

                    // Log after scraping reports for this company is finished
                    Console.WriteLine($"[INFO] Finished scraping reports for {company.companyName} ({company.symbol})");

                    // Call CheckAndFillMissingFinancialYears for this company
                    await semaphore.WaitAsync(); // Ensuring proper sequential execution
                    try
                    {
                        Console.WriteLine($"[INFO] Calling CheckAndFillMissingFinancialYears for {company.companyName} ({company.symbol})");
                        Console.WriteLine($"[INFO] Completed CheckAndFillMissingFinancialYears for {company.companyName} ({company.symbol})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Exception while processing CheckAndFillMissingFinancialYears for {company.companyName}: {ex.Message}");
                    }
                    finally
                    {
                        semaphore.Release(); // Release the lock after finishing
                    }
                }

                Console.WriteLine("[INFO] All companies' reports processed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Scraping and processing failed: {ex.Message}");
                throw; // Rethrow the exception for further handling
            }
            finally
            {
                Console.WriteLine("[INFO] ScrapeAndProcessDataAsync fully completed.");
            }
        }
    }
}


//using DataElements;
//using HtmlAgilityPack;
//using Newtonsoft.Json;
//using OpenQA.Selenium;
//using OpenQA.Selenium.Chrome;
//using OpenQA.Selenium.Support.UI;
//using SeleniumExtras.WaitHelpers;
//using System.Data.SqlClient;
//using System.Diagnostics;
//using System.Globalization;
//using System.Xml.Linq;
//using static DataElements.FinancialElementLists;

//namespace Nasdaq100FinancialScraper
//{
//    class Program
//    {
//        public static readonly string connectionString = "Server=LAPTOP-871MLHAT\\sqlexpress;Database=StockDataScraperDatabase;Integrated Security=True;";
//        public static DateTime? globalStartDate = null;
//        public static DateTime? globalEndDate = null;
//        public static DateTime? globalInstantDate = null;
//        public static List<SqlCommand> batchedCommands = new List<SqlCommand>();
//        private static readonly HttpClient httpClient = new HttpClient();
//        public static readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1); // Ensuring proper sequential execution

//        private static bool wasCheckAndFillCalled = false; // Track if method was called

//        static async Task Main(string[] args)
//        {
//            try
//            {
//                Console.WriteLine("[INFO] Starting the program.");
//                await ScrapeAndProcessDataAsync(); // Ensure this logic is awaited
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine("[INFO] Calling CheckAndFillMissingFinancialYears for all companies.");
//                await CheckAndFillMissingFinancialYears(); // This will now process for all companies

//                // Confirm the method was hit
//                Console.WriteLine($"[INFO] Was CheckAndFillMissingFinancialYears called? {wasCheckAndFillCalled}");
//            }
//        }
//        private static async Task CheckAndFillMissingFinancialYears(int companyId)
//        {
//            Console.WriteLine($"[INFO] Entered CheckAndFillMissingFinancialYears for CompanyID: {companyId}");

//            try
//            {
//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    Console.WriteLine("[INFO] Connection to database opened.");

//                    string query = @"
//                SELECT Year, Quarter, EndDate
//                FROM FinancialData
//                WHERE CompanyID = @CompanyID AND (FinancialYear IS NULL OR FinancialYear = 0)
//                ORDER BY Year, Quarter ASC";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    {
//                        command.Parameters.AddWithValue("@CompanyID", companyId);

//                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                        {
//                            List<(int Year, int Quarter, DateTime EndDate)> missingRows = new List<(int, int, DateTime)>();

//                            while (await reader.ReadAsync())
//                            {
//                                int year = reader.GetInt32(0);
//                                int quarter = reader.GetInt32(1);
//                                DateTime endDate = reader.GetDateTime(2);
//                                missingRows.Add((year, quarter, endDate));
//                            }

//                            foreach (var row in missingRows)
//                            {
//                                // Call the new method to calculate the financial year
//                                int financialYear = GetFinancialYearForQuarter(row.Quarter, row.EndDate);
//                                await UpdateFinancialYear(companyId, row.Year, row.Quarter, financialYear, connection);
//                            }
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears for CompanyID={companyId}: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine($"[INFO] Completed CheckAndFillMissingFinancialYears for CompanyID={companyId}");
//            }
//        }

//        private static int GetFinancialYearForQuarter(int quarter, DateTime endDate)
//        {
//            // Based on the quarter, we add the respective months to calculate the financial year
//            switch (quarter)
//            {
//                case 1: // Q1: Financial year is the EndDate year + 9 months
//                    return endDate.AddMonths(9).Year;
//                case 2: // Q2: Financial year is the EndDate year + 6 months
//                    return endDate.AddMonths(6).Year;
//                case 3: // Q3: Financial year is the EndDate year + 3 months
//                    return endDate.AddMonths(3).Year;
//                case 4: // Q4: Financial year is the EndDate year (No change)
//                    return endDate.Year;
//                default:
//                    throw new InvalidOperationException("Invalid quarter value: " + quarter);
//            }
//        }
//        private static async Task ProcessAndUpdateQuarter(int year, int quarter, DateTime endDate, int companyId, SqlConnection connection)
//        {
//            // Step 3: Calculate the financial year for this quarter using the new method
//            int financialYear = GetFinancialYearForQuarter(quarter, endDate);

//            if (financialYear == 0)
//            {
//                Console.WriteLine("[INFO] No FinancialYear found, setting to 0.");
//                financialYear = 0;
//            }

//            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//            Console.WriteLine($"[INFO] Updated FinancialYear to {financialYear} for CompanyID={companyId}, Year={year}, Quarter={quarter}");
//        }
//        private static async Task UpdateFinancialYear(int companyId, int year, int quarter, int financialYear, SqlConnection connection)
//        {
//            string updateQuery = @"
//    UPDATE FinancialData
//    SET FinancialYear = @FinancialYear
//    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//            using (SqlCommand command = new SqlCommand(updateQuery, connection))
//            {
//                command.Parameters.AddWithValue("@FinancialYear", financialYear);
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", quarter);

//                await command.ExecuteNonQueryAsync();
//                Console.WriteLine($"[INFO] Updated FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter} to {financialYear}");
//            }
//        }
//        private static async Task ProcessQuarter(int year, int quarter, DateTime endDate, int companyId, SqlConnection connection)
//        {
//            int financialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);

//            if (financialYear == 0)
//            {
//                Console.WriteLine("[INFO] No FinancialYear found, setting to 0.");
//                financialYear = 0;
//            }

//            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//            Console.WriteLine($"[INFO] Updated FinancialYear to {financialYear} for CompanyID={companyId}, Year={year}, Quarter={quarter}");
//        }

//        private static async Task PropagateFinancialYears(int companyId, SqlConnection connection)
//        {
//            // Propagate the calculated financial year to adjacent quarters (both backwards and forwards)
//            // Logic to fetch the next and previous quarters and update missing FinancialYear
//            string query = @"
//        SELECT Year, Quarter, FinancialYear
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID
//        ORDER BY Year, Quarter";

//            using (SqlCommand command = new SqlCommand(query, connection))
//            {
//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    List<(int Year, int Quarter, int FinancialYear)> rows = new List<(int, int, int)>();

//                    while (await reader.ReadAsync())
//                    {
//                        int year = reader.GetInt32(0);
//                        int quarter = reader.GetInt32(1);
//                        int financialYear = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

//                        rows.Add((year, quarter, financialYear));
//                    }

//                    // Forward and backward propagation logic
//                    for (int i = 0; i < rows.Count; i++)
//                    {
//                        if (rows[i].FinancialYear == 0)
//                        {
//                            // Try to propagate from previous quarter
//                            if (i > 0 && rows[i - 1].FinancialYear > 0)
//                            {
//                                rows[i] = (rows[i].Year, rows[i].Quarter, rows[i - 1].FinancialYear);
//                                await UpdateFinancialYear(companyId, rows[i].Year, rows[i].Quarter, rows[i].FinancialYear, connection);
//                            }
//                            // Try to propagate from next quarter
//                            else if (i < rows.Count - 1 && rows[i + 1].FinancialYear > 0)
//                            {
//                                rows[i] = (rows[i].Year, rows[i].Quarter, rows[i + 1].FinancialYear);
//                                await UpdateFinancialYear(companyId, rows[i].Year, rows[i].Quarter, rows[i].FinancialYear, connection);
//                            }
//                        }
//                    }
//                }
//            }
//        }

//        private static async Task PropagateFinancialYear(int companyId, int year, int quarter, SqlConnection connection, int direction)
//        {
//            // Check if quarter is within bounds
//            if (quarter < 1 || quarter > 4)
//            {
//                if (quarter < 1) // If previous year should be checked
//                {
//                    year -= 1;
//                    quarter = 4;
//                }
//                else if (quarter > 4) // If next year should be checked
//                {
//                    year += 1;
//                    quarter = 1;
//                }
//            }

//            // Query to find adjacent quarter and check if FinancialYear is missing
//            string checkQuery = @"
//    SELECT FinancialYear, Year, Quarter
//    FROM FinancialData
//    WHERE CompanyID = @CompanyID AND ((Year = @Year AND Quarter = @Quarter) OR (Year = @PreviousYear AND Quarter = 4) OR (Year = @NextYear AND Quarter = 1))
//    ORDER BY Year, Quarter";  // Order by Year and Quarter for correctness

//            using (SqlCommand command = new SqlCommand(checkQuery, connection))
//            {
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", quarter);
//                command.Parameters.AddWithValue("@PreviousYear", year - 1);
//                command.Parameters.AddWithValue("@NextYear", year + 1);

//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    if (reader.Read())
//                    {
//                        int financialYear = reader.GetInt32(0);
//                        if (financialYear == 0) // FinancialYear is missing
//                        {
//                            Console.WriteLine($"[INFO] Filling missing FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter}");

//                            // Calculate FinancialYear for the missing quarter
//                            int calculatedYear = GetFinancialYearForReport(companyId, new DateTime(year, quarter * 3, 1), quarter, connection);

//                            if (calculatedYear > 0)
//                            {
//                                await UpdateFinancialYear(companyId, year, quarter, calculatedYear, connection);

//                                // Recursively propagate to the adjacent quarters in the same direction
//                                await PropagateFinancialYear(companyId, year, quarter + direction, connection, direction);
//                            }
//                        }
//                    }
//                }
//            }
//        }

//        private static async Task CheckAdjacentQuarters(int companyId, int year, int quarter, SqlConnection connection)
//        {
//            // Check previous quarter
//            await PropagateFinancialYear(companyId, year, quarter - 1, connection, -1);

//            // Check next quarter
//            await PropagateFinancialYear(companyId, year, quarter + 1, connection, 1);
//        }


//        private static async Task TryFillFinancialYear(int companyId, int year, int quarter, SqlConnection connection)
//        {
//            // Check if the financial year for the quarter is missing (NULL)
//            string checkQuery = @"
//    SELECT FinancialYear, EndDate
//    FROM FinancialData
//    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//            using (SqlCommand checkCommand = new SqlCommand(checkQuery, connection))
//            {
//                checkCommand.Parameters.AddWithValue("@CompanyID", companyId);
//                checkCommand.Parameters.AddWithValue("@Year", year);
//                checkCommand.Parameters.AddWithValue("@Quarter", quarter);

//                using (SqlDataReader reader = await checkCommand.ExecuteReaderAsync())
//                {
//                    if (await reader.ReadAsync())
//                    {
//                        var financialYear = reader.IsDBNull(0) ? (int?)null : reader.GetInt32(0);
//                        DateTime endDate = reader.GetDateTime(1);

//                        // If the financial year is missing, calculate and update it
//                        if (financialYear == null || financialYear == 0)
//                        {
//                            reader.Close();  // Make sure to close the reader before executing another command

//                            int newFinancialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);
//                            if (newFinancialYear > 0)
//                            {
//                                await UpdateFinancialYear(companyId, year, quarter, newFinancialYear, connection);
//                            }
//                        }
//                    }
//                }
//            }
//        }        
//        private static int GetFinancialYearForReport(int companyId, DateTime quarterEndDate, int quarter, SqlConnection connection)
//        {
//            // First, try to get the financial year from adjacent reports
//            int financialYear = GetFinancialYearFromAdjacentReports(companyId, quarterEndDate, connection, null);

//            // Fallback to fiscal year calculation if adjacent reports didn't yield a result
//            if (financialYear == 0)
//            {
//                financialYear = CalculateFinancialYear(companyId, quarterEndDate, connection, null);
//            }

//            // If both methods return 0, set the financialYear to 0
//            if (financialYear == 0)
//            {
//                Console.WriteLine($"[INFO] No financial year determined for CompanyID: {companyId}. Setting FinancialYear to 0.");
//                financialYear = 0;
//            }

//            return financialYear;
//        }
//        // Add this method to check adjacent quarters and fill missing FinancialYear values
//        private static async Task CheckAdjacentQuarters(int companyId, int year, int quarter, int financialYear, SqlConnection connection)
//        {
//            // Check previous quarter
//            if (quarter > 1)  // Only check if it's not the first quarter
//            {
//                int previousQuarter = quarter - 1;
//                int prevFinancialYear = GetFinancialYearForReport(companyId, year, previousQuarter, connection);

//                if (prevFinancialYear == 0)
//                {
//                    Console.WriteLine($"[INFO] Filling missing FinancialYear for previous quarter: CompanyID={companyId}, Year={year}, Quarter={previousQuarter}");
//                    prevFinancialYear = CalculateFinancialYear(companyId, new DateTime(year, (previousQuarter * 3), 1), connection, null);  // Recalculate based on adjacent report

//                    if (prevFinancialYear > 0)
//                    {
//                        await UpdateFinancialYear(companyId, year, previousQuarter, prevFinancialYear, connection);
//                    }
//                }
//            }

//            // Check next quarter
//            if (quarter < 4)  // Only check if it's not the last quarter
//            {
//                int nextQuarter = quarter + 1;
//                int nextFinancialYear = GetFinancialYearForReport(companyId, year, nextQuarter, connection);

//                if (nextFinancialYear == 0)
//                {
//                    Console.WriteLine($"[INFO] Filling missing FinancialYear for next quarter: CompanyID={companyId}, Year={year}, Quarter={nextQuarter}");
//                    nextFinancialYear = CalculateFinancialYear(companyId, new DateTime(year, (nextQuarter * 3), 1), connection, null);  // Recalculate based on adjacent report

//                    if (nextFinancialYear > 0)
//                    {
//                        await UpdateFinancialYear(companyId, year, nextQuarter, nextFinancialYear, connection);
//                    }
//                }
//            }
//        }

//        // Modify the existing GetFinancialYearForReport to handle adjacent reports effectively
//        private static int GetFinancialYearForReport(int companyId, int year, int quarter, SqlConnection connection)
//        {
//            DateTime quarterEndDate = new DateTime(year, (quarter * 3), 1).AddMonths(1).AddDays(-1);
//            int financialYear = GetFinancialYearFromAdjacentReports(companyId, quarterEndDate, connection, null);

//            // Fallback to fiscal year calculation if adjacent reports didn't yield a result
//            if (financialYear == 0)
//            {
//                financialYear = CalculateFinancialYear(companyId, quarterEndDate, connection, null);
//            }

//            // If both methods return 0, set the financialYear to 0
//            if (financialYear == 0)
//            {
//                Console.WriteLine($"[INFO] No financial year determined for CompanyID: {companyId}, Year: {year}, Quarter: {quarter}. Setting FinancialYear to 0.");
//                financialYear = 0;
//            }

//            return financialYear;
//        }


//        private static async Task ExecuteBatchedCommands(SqlConnection connection)
//        {
//            if (batchedCommands.Count > 0)
//            {
//                using (SqlTransaction transaction = connection.BeginTransaction())
//                {
//                    try
//                    {
//                        foreach (var command in batchedCommands)
//                        {
//                            command.Connection = connection;
//                            command.Transaction = transaction;
//                            await command.ExecuteNonQueryAsync();
//                        }

//                        transaction.Commit();
//                        Console.WriteLine("[INFO] Batch executed and committed.");
//                    }
//                    catch (Exception ex)
//                    {
//                        transaction.Rollback();
//                        Console.WriteLine($"[ERROR] Failed to execute batched commands: {ex.Message}");
//                    }
//                    finally
//                    {
//                        batchedCommands.Clear(); // Clear after execution
//                    }
//                }
//            }
//        }

       

//        // Method for filling missing financial years across all companies (post-scraping)
//        private static async Task CheckAndFillMissingFinancialYears()
//        {
//            Console.WriteLine("[INFO] Entered CheckAndFillMissingFinancialYears for all companies.");
//            wasCheckAndFillCalled = true; // Set the flag when the method is called

//            try
//            {
//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    Console.WriteLine("[INFO] Connection to database opened.");

//                    string query = @"
//                    SELECT CompanyID, Year, Quarter, EndDate
//                    FROM FinancialData
//                    WHERE FinancialYear IS NULL OR FinancialYear = 0";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                    {
//                        if (!reader.HasRows)
//                        {
//                            Console.WriteLine("[INFO] No rows with missing FinancialYear found.");
//                        }

//                        while (await reader.ReadAsync())
//                        {
//                            int companyId = reader.GetInt32(0);
//                            int year = reader.GetInt32(1);
//                            int quarter = reader.GetInt32(2);
//                            DateTime endDate = reader.GetDateTime(3);

//                            Console.WriteLine($"[INFO] Processing row: CompanyID={companyId}, Year={year}, Quarter={quarter}, EndDate={endDate}");

//                            int financialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);

//                            if (financialYear == 0)
//                            {
//                                Console.WriteLine("[INFO] No FinancialYear found, setting to 0.");
//                                financialYear = 0;
//                            }

//                            if (financialYear >= 0)
//                            {
//                                await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//                                Console.WriteLine($"[INFO] Updated FinancialYear to {financialYear} for CompanyID={companyId}");
//                            }
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine($"[INFO] CheckAndFillMissingFinancialYears was called: {wasCheckAndFillCalled}");
//            }
//        }

//        private static int CalculateFinancialYear(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            DateTime fiscalYearEnd = GetFiscalYearEndForSpecificYear(companyId, quarterEndDate.Year, connection, transaction);
//            DateTime fiscalYearStart = fiscalYearEnd.AddYears(-1).AddDays(1);

//            // Add more logging to check what fiscal year range is being used
//            Console.WriteLine($"[DEBUG] Fiscal Year Start: {fiscalYearStart}, Fiscal Year End: {fiscalYearEnd}, Quarter End Date: {quarterEndDate}");

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

//        private static int GetFinancialYearFromAdjacentReports(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            int financialYear = 0;

//            // Check next quarterly report
//            financialYear = GetFinancialYearFromNextQuarterlyReport(companyId, quarterEndDate, connection, transaction);

//            // If next quarterly report didn't help, check previous quarterly report
//            if (financialYear == 0)
//            {
//                financialYear = GetFinancialYearFromPreviousQuarterlyReport(companyId, quarterEndDate, connection, transaction);
//            }

//            return financialYear;
//        }
//        private static DateTime GetFiscalYearEndForSpecificYear(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//                    SELECT TOP 1 EndDate
//                    FROM FinancialData
//                    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 0
//                    ORDER BY EndDate DESC";

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
//       private static readonly SemaphoreSlim chromeSemaphore = new SemaphoreSlim(3, 3); // Semaphore for controlling ChromeDriver instances
       
//        private static int GetFinancialYearFromPreviousQuarterlyReport(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                // Add detailed logging for debugging
//                Console.WriteLine($"[DEBUG] Running query for previous quarterly report. CompanyID: {companyId}, QuarterEndDate: {quarterEndDate}");

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND EndDate < @QuarterEndDate AND Quarter > 0
//        ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@QuarterEndDate", quarterEndDate);

//                var result = command.ExecuteScalar();

//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    Console.WriteLine($"[DEBUG] Found previous quarterly report. FinancialYear: {financialYear}, CompanyID: {companyId}, EndDate: {quarterEndDate}");
//                    return financialYear;
//                }
//                else
//                {
//                    Console.WriteLine($"[DEBUG] No previous quarterly report found for CompanyID: {companyId}, before {quarterEndDate}");
//                    return 0;
//                }
//            }
//        }
//        private static int GetFinancialYearFromNextQuarterlyReport(int companyId, DateTime currentEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                // Add detailed logging for debugging
//                Console.WriteLine($"[DEBUG] Running query for next quarterly report. CompanyID: {companyId}, CurrentEndDate: {currentEndDate}");

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND EndDate > DATEADD(day, -10, @CurrentEndDate) AND Quarter > 0
//        ORDER BY EndDate ASC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@CurrentEndDate", currentEndDate);

//                var result = command.ExecuteScalar();

//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    Console.WriteLine($"[DEBUG] Found next quarterly report. FinancialYear: {financialYear}, CompanyID: {companyId}, EndDate: {currentEndDate}");
//                    return financialYear;
//                }
//                else
//                {
//                    Console.WriteLine($"[DEBUG] No next quarterly report found for CompanyID: {companyId}, after {currentEndDate}");
//                    return 0;
//                }
//            }
//        }
//        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                Console.WriteLine($"[DEBUG] SaveQuarterData called. CompanyID: {companyId}, Quarter: {quarter}, ElementName: {elementName}");

//                // Try to get the financial year from adjacent reports
//                int financialYear = GetFinancialYearFromAdjacentReports(companyId, endDate, connection, transaction);

//                // If no financial year was found, fall back to fiscal year calculation
//                if (financialYear == 0)
//                {
//                    Console.WriteLine($"[WARNING] Could not fetch financial year from adjacent reports for CompanyID: {companyId}, Quarter: {quarter}. Falling back to fiscal year calculation.");
//                    financialYear = CalculateFinancialYear(companyId, endDate, connection, transaction);
//                }

//                // Ensure financialYear is set to 0 if not determined
//                if (financialYear == 0)
//                {
//                    Console.WriteLine($"[INFO] No financial year determined for CompanyID: {companyId}. Setting FinancialYear to 0.");
//                }

//                DateTime startDate = quarter == 0 ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);

//                Console.WriteLine($"[INFO] Handling quarterly report for company {companyId}. Financial year set to {financialYear}");

//                // Fetch or create the PeriodID from the Periods table
//                command.CommandText = @"
//            SELECT TOP 1 PeriodID 
//            FROM Periods 
//            WHERE Year = @Year AND Quarter = @Quarter";
//                command.Parameters.AddWithValue("@Year", financialYear);
//                command.Parameters.AddWithValue("@Quarter", quarter);

//                object result = command.ExecuteScalar();
//                if (result == null)
//                {
//                    Console.WriteLine($"[ERROR] Could not find corresponding period for CompanyID: {companyId}, Quarter: {quarter}, Financial Year: {financialYear}");
//                    return;
//                }

//                int periodId = Convert.ToInt32(result);

//                // Prepare insert or update command
//                command.Parameters.Clear();
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@PeriodID", periodId);
//                command.Parameters.AddWithValue("@FinancialYear", financialYear); // This ensures 0 is saved if financialYear is 0
//                command.Parameters.AddWithValue("@Year", endDate.Year);
//                command.Parameters.AddWithValue("@Quarter", quarter);
//                command.Parameters.AddWithValue("@StartDate", startDate);
//                command.Parameters.AddWithValue("@EndDate", endDate);
//                command.Parameters.AddWithValue("@Value", value);

//                // Handle Annual or Quarterly reports
//                if (elementName == "AnnualReport")
//                {
//                    command.CommandText = @"
//                IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
//                BEGIN
//                    UPDATE FinancialData 
//                    SET FinancialYear = @FinancialYear, Year = @Year, Quarter = 0, StartDate = @StartDate, EndDate = @EndDate
//                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
//                END
//                ELSE
//                BEGIN
//                    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
//                    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, 0, @StartDate, @EndDate);
//                END";
//                }
//                else
//                {
//                    command.CommandText = @"
//                IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
//                BEGIN
//                    UPDATE FinancialData 
//                    SET FinancialYear = @FinancialYear, Year = @Year, Quarter = @Quarter, StartDate = @StartDate, EndDate = @EndDate 
//                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
//                END
//                ELSE
//                BEGIN
//                    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
//                    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate);
//                END";
//                }

//                // Add command to batch
//                batchedCommands.Add(command);
//                Console.WriteLine($"[INFO] Added command to batch for CompanyID: {companyId}, Quarter: {quarter}, FinancialYear: {financialYear}");
//            }
//        }
//        private static async Task FillMissingFinancialYearsAsync()
//        {
//            try
//            {
//                Console.WriteLine("[INFO] Starting to fill missing financial years.");

//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    string query = @"
//                SELECT CompanyID, Year, Quarter, EndDate
//                FROM FinancialData
//                WHERE FinancialYear IS NULL";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                    {
//                        while (await reader.ReadAsync())
//                        {
//                            int companyId = reader.GetInt32(0);
//                            int year = reader.GetInt32(1);
//                            int quarter = reader.GetInt32(2);
//                            DateTime endDate = reader.GetDateTime(3);

//                            Console.WriteLine($"[INFO] Processing CompanyID={companyId}, Year={year}, Quarter={quarter}, EndDate={endDate}");

//                            int financialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);

//                            // Explicitly set 0 if no financial year is determined
//                            if (financialYear == 0)
//                            {
//                                financialYear = 0;
//                                Console.WriteLine($"[INFO] FinancialYear is not determined. Setting to 0 for CompanyID={companyId}");
//                            }

//                            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//                        }
//                    }
//                }

//                Console.WriteLine("[INFO] Successfully filled missing financial years.");
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Error filling missing financial years: {ex.Message}");
//            }
//        }

//        private static async Task RecalculateMissingFinancialYears()
//        {
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();

//                // Query to find rows with missing financial years
//                string query = @"
//            SELECT CompanyID, Year, Quarter, EndDate
//            FROM FinancialData
//            WHERE FinancialYear IS NULL";

//                using (SqlCommand command = new SqlCommand(query, connection))
//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    while (await reader.ReadAsync())
//                    {
//                        int companyId = reader.GetInt32(0);
//                        int year = reader.GetInt32(1);
//                        int quarter = reader.GetInt32(2);
//                        DateTime endDate = reader.GetDateTime(3);

//                        // Recalculate financial year based on company fiscal year
//                        int financialYear = CalculateFinancialYear(companyId, endDate, connection, null);

//                        // Update the financial year
//                        using (SqlCommand updateCommand = new SqlCommand(@"UPDATE FinancialData SET FinancialYear = @FinancialYear WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter", connection))
//                        {
//                            updateCommand.Parameters.AddWithValue("@FinancialYear", financialYear);
//                            updateCommand.Parameters.AddWithValue("@CompanyID", companyId);
//                            updateCommand.Parameters.AddWithValue("@Year", year);
//                            updateCommand.Parameters.AddWithValue("@Quarter", quarter);

//                            await updateCommand.ExecuteNonQueryAsync();
//                        }
//                    }
//                }
//            }
//        }
//        private static async Task MergeDuplicateRows()
//        {
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                string findDuplicatesQuery = @"
//            SELECT CompanyID, StartDate, EndDate, COUNT(*) AS DuplicateCount
//            FROM FinancialData
//            GROUP BY CompanyID, StartDate, EndDate
//            HAVING COUNT(*) > 1";

//                using (SqlCommand findDuplicatesCommand = new SqlCommand(findDuplicatesQuery, connection))
//                using (SqlDataReader reader = await findDuplicatesCommand.ExecuteReaderAsync())
//                {
//                    while (await reader.ReadAsync())
//                    {
//                        int companyId = reader.GetInt32(0);
//                        DateTime startDate = reader.GetDateTime(1);
//                        DateTime endDate = reader.GetDateTime(2);

//                        // Step 2: Consolidate data for the duplicates
//                        await ConsolidateDuplicateRows(companyId, startDate, endDate, connection);
//                    }
//                }
//            }
//        }
//        private static async Task ConsolidateDuplicateRows(int companyId, DateTime startDate, DateTime endDate, SqlConnection connection)
//        {
//            string fetchDuplicatesQuery = @"
//        SELECT FinancialDataID, PeriodID, Value
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID AND StartDate = @StartDate AND EndDate = @EndDate";

//            using (SqlCommand fetchCommand = new SqlCommand(fetchDuplicatesQuery, connection))
//            {
//                fetchCommand.Parameters.AddWithValue("@CompanyID", companyId);
//                fetchCommand.Parameters.AddWithValue("@StartDate", startDate);
//                fetchCommand.Parameters.AddWithValue("@EndDate", endDate);

//                using (SqlDataReader reader = await fetchCommand.ExecuteReaderAsync())
//                {
//                    decimal consolidatedValue = 0;
//                    List<int> duplicateIds = new List<int>();

//                    while (await reader.ReadAsync())
//                    {
//                        int financialDataId = reader.GetInt32(reader.GetOrdinal("FinancialDataID"));
//                        decimal value = reader.GetDecimal(reader.GetOrdinal("Value"));
//                        consolidatedValue += value; // Aggregate values (can be customized based on your requirements)
//                        duplicateIds.Add(financialDataId); // Collect FinancialDataID for deletion
//                    }

//                    if (duplicateIds.Count > 0)
//                    {
//                        // Step 2b: Update one row with the consolidated value
//                        await UpdateConsolidatedRow(duplicateIds[0], consolidatedValue, connection);

//                        // Step 2c: Delete the duplicate rows except the one we updated
//                        await DeleteDuplicateRows(duplicateIds.Skip(1).ToList(), connection);
//                    }
//                }
//            }
//        }
//        private static async Task UpdateConsolidatedRow(int financialDataId, decimal consolidatedValue, SqlConnection connection)
//        {
//            string updateQuery = @"
//        UPDATE FinancialData
//        SET Value = @Value
//        WHERE FinancialDataID = @FinancialDataID";

//            using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection))
//            {
//                updateCommand.Parameters.AddWithValue("@FinancialDataID", financialDataId);
//                updateCommand.Parameters.AddWithValue("@Value", consolidatedValue);

//                await updateCommand.ExecuteNonQueryAsync();
//            }
//        }
//        private static async Task DeleteDuplicateRows(List<int> duplicateIds, SqlConnection connection)
//        {
//            if (duplicateIds.Count > 0)
//            {
//                string deleteQuery = @"
//            DELETE FROM FinancialData
//            WHERE FinancialDataID = @FinancialDataID";

//                foreach (int id in duplicateIds)
//                {
//                    using (SqlCommand deleteCommand = new SqlCommand(deleteQuery, connection))
//                    {
//                        deleteCommand.Parameters.AddWithValue("@FinancialDataID", id);
//                        await deleteCommand.ExecuteNonQueryAsync();
//                    }
//                }
//            }
//        }
//        private static int GetFinancialYearFromAnnualReport(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//                SELECT TOP 1 FinancialYear 
//                FROM FinancialData 
//                WHERE CompanyID = @CompanyID AND Quarter = 0
//                ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                object result = command.ExecuteScalar();
//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    return financialYear;
//                }
//                else
//                {
//                    return quarterEndDate.Year;
//                }
//            }
//        }
//        private static DateTime GetFiscalYearEnd(int companyId, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//        SELECT TOP 1 EndDate 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND Quarter = 0
//        ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                object result = command.ExecuteScalar();
//                if (result != null && DateTime.TryParse(result.ToString(), out DateTime fiscalYearEnd))
//                {
//                    Console.WriteLine($"[DEBUG] Fiscal Year End fetched from database for Company {companyId}: {fiscalYearEnd}");
//                    return fiscalYearEnd;
//                }
//                else
//                {
//                    Console.WriteLine($"[ERROR] No fiscal year end date found for Company {companyId}, defaulting to December 31 of current year.");
//                    return new DateTime(DateTime.Now.Year, 12, 31);
//                }
//            }
//        }
//        private static int GetFinancialYearFromNextQuarter(int companyId, int year, int nextQuarter, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", nextQuarter);

//                object result = command.ExecuteScalar();
//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    return financialYear;
//                }
//                return 0; // Return 0 if no financial year found for the next quarter
//            }
//        }
//        private static int GetFinancialYearFromPreviousQuarter(int companyId, int year, int previousQuarter, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", previousQuarter);

//                object result = command.ExecuteScalar();
//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    return financialYear;
//                }
//                return 0; // Return 0 if no financial year found for the previous quarter
//            }
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
//                    IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
//                    BEGIN
//                        INSERT INTO CompaniesList (CompanyName, CompanySymbol, CIK) 
//                        VALUES (@CompanyName, @CompanySymbol, @CIK);
//                    END";

//                        command.Parameters.AddWithValue("@CompanyName", company.CompanyName);
//                        command.Parameters.AddWithValue("@CompanySymbol", company.Ticker);
//                        if (int.TryParse(company.CIK, out int cikValue))
//                        {
//                            command.Parameters.AddWithValue("@CIK", cikValue);
//                        }
//                        else
//                        {
//                            Console.WriteLine($"Invalid CIK value for {company.CompanyName} ({company.Ticker}): {company.CIK}");
//                            continue; // Skip this record if CIK is not valid
//                        }

//                        await command.ExecuteNonQueryAsync();
//                    }
//                }
//            }
//        }       
//    }
//}


//using DataElements;
//using HtmlAgilityPack;
//using Newtonsoft.Json;
//using OpenQA.Selenium;
//using OpenQA.Selenium.Chrome;
//using OpenQA.Selenium.Support.UI;
//using SeleniumExtras.WaitHelpers;
//using System.Data.SqlClient;
//using System.Diagnostics;
//using System.Globalization;
//using System.Xml.Linq;
//using static DataElements.FinancialElementLists;

//namespace Nasdaq100FinancialScraper
//{
//    class Program
//    {
//        public static readonly string connectionString = "Server=LAPTOP-871MLHAT\\sqlexpress;Database=StockDataScraperDatabase;Integrated Security=True;";
//        public static DateTime? globalStartDate = null;
//        public static DateTime? globalEndDate = null;
//        public static DateTime? globalInstantDate = null;
//        public static List<SqlCommand> batchedCommands = new List<SqlCommand>();
//        private static readonly HttpClient httpClient = new HttpClient();
//        public static readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1); // Ensuring proper sequential execution

//        private static bool wasCheckAndFillCalled = false; // Track if method was called

//        static async Task Main(string[] args)
//        {
//            try
//            {
//                Console.WriteLine("[INFO] Starting the program.");
//                await ScrapeAndProcessDataAsync(); // Ensure this logic is awaited
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine("[INFO] Calling CheckAndFillMissingFinancialYears for all companies.");
//                await CheckAndFillMissingFinancialYears(); // This will now process for all companies

//                // Confirm the method was hit
//                Console.WriteLine($"[INFO] Was CheckAndFillMissingFinancialYears called? {wasCheckAndFillCalled}");
//            }
//        }
//        private static int GetFinancialYearForQuarter(int quarter, DateTime endDate)
//        {
//            // Based on the quarter, we add the respective months to calculate the financial year
//            switch (quarter)
//            {
//                case 1: // Q1: Financial year is the EndDate year + 9 months
//                    return endDate.AddMonths(9).Year;
//                case 2: // Q2: Financial year is the EndDate year + 6 months
//                    return endDate.AddMonths(6).Year;
//                case 3: // Q3: Financial year is the EndDate year + 3 months
//                    return endDate.AddMonths(3).Year;
//                case 4: // Q4: Financial year is the EndDate year (No change)
//                    return endDate.Year;
//                default:
//                    throw new InvalidOperationException("Invalid quarter value: " + quarter);
//            }
//        }
//        private static async Task ProcessAndUpdateQuarter(int year, int quarter, DateTime endDate, int companyId, SqlConnection connection)
//        {
//            // Step 3: Calculate the financial year for this quarter using the new method
//            int financialYear = GetFinancialYearForQuarter(quarter, endDate);

//            if (financialYear == 0)
//            {
//                Console.WriteLine("[INFO] No FinancialYear found, setting to 0.");
//                financialYear = 0;
//            }

//            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//            Console.WriteLine($"[INFO] Updated FinancialYear to {financialYear} for CompanyID={companyId}, Year={year}, Quarter={quarter}");
//        }
//        private static async Task CheckAndFillMissingFinancialYears(int companyId)
//        {
//            Console.WriteLine($"[INFO] Entered CheckAndFillMissingFinancialYears for CompanyID: {companyId}");

//            try
//            {
//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    Console.WriteLine("[INFO] Connection to database opened.");

//                    string query = @"
//                SELECT Year, Quarter, EndDate
//                FROM FinancialData
//                WHERE CompanyID = @CompanyID AND (FinancialYear IS NULL OR FinancialYear = 0)
//                ORDER BY Year, Quarter ASC";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    {
//                        command.Parameters.AddWithValue("@CompanyID", companyId);

//                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                        {
//                            List<(int Year, int Quarter, DateTime EndDate)> missingRows = new List<(int, int, DateTime)>();

//                            while (await reader.ReadAsync())
//                            {
//                                int year = reader.GetInt32(0);
//                                int quarter = reader.GetInt32(1);
//                                DateTime endDate = reader.GetDateTime(2);
//                                missingRows.Add((year, quarter, endDate));
//                            }

//                            foreach (var row in missingRows)
//                            {
//                                await ProcessAndUpdateQuarter(row.Year, row.Quarter, row.EndDate, companyId, connection);
//                            }
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears for CompanyID={companyId}: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine($"[INFO] Completed CheckAndFillMissingFinancialYears for CompanyID={companyId}");
//            }
//        }
//        private static async Task UpdateFinancialYear(int companyId, int year, int quarter, int financialYear, SqlConnection connection)
//        {
//            string updateQuery = @"
//    UPDATE FinancialData
//    SET FinancialYear = @FinancialYear
//    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//            using (SqlCommand command = new SqlCommand(updateQuery, connection))
//            {
//                command.Parameters.AddWithValue("@FinancialYear", financialYear);
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", quarter);

//                await command.ExecuteNonQueryAsync();
//                Console.WriteLine($"[INFO] Updated FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter} to {financialYear}");
//            }
//        }

//        private static async Task ProcessQuarter(int year, int quarter, DateTime endDate, int companyId, SqlConnection connection)
//        {
//            int financialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);

//            if (financialYear == 0)
//            {
//                Console.WriteLine("[INFO] No FinancialYear found, setting to 0.");
//                financialYear = 0;
//            }

//            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//            Console.WriteLine($"[INFO] Updated FinancialYear to {financialYear} for CompanyID={companyId}, Year={year}, Quarter={quarter}");
//        }

//        private static async Task PropagateFinancialYears(int companyId, SqlConnection connection)
//        {
//            // Propagate the calculated financial year to adjacent quarters (both backwards and forwards)
//            // Logic to fetch the next and previous quarters and update missing FinancialYear
//            string query = @"
//        SELECT Year, Quarter, FinancialYear
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID
//        ORDER BY Year, Quarter";

//            using (SqlCommand command = new SqlCommand(query, connection))
//            {
//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    List<(int Year, int Quarter, int FinancialYear)> rows = new List<(int, int, int)>();

//                    while (await reader.ReadAsync())
//                    {
//                        int year = reader.GetInt32(0);
//                        int quarter = reader.GetInt32(1);
//                        int financialYear = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

//                        rows.Add((year, quarter, financialYear));
//                    }

//                    // Forward and backward propagation logic
//                    for (int i = 0; i < rows.Count; i++)
//                    {
//                        if (rows[i].FinancialYear == 0)
//                        {
//                            // Try to propagate from previous quarter
//                            if (i > 0 && rows[i - 1].FinancialYear > 0)
//                            {
//                                rows[i] = (rows[i].Year, rows[i].Quarter, rows[i - 1].FinancialYear);
//                                await UpdateFinancialYear(companyId, rows[i].Year, rows[i].Quarter, rows[i].FinancialYear, connection);
//                            }
//                            // Try to propagate from next quarter
//                            else if (i < rows.Count - 1 && rows[i + 1].FinancialYear > 0)
//                            {
//                                rows[i] = (rows[i].Year, rows[i].Quarter, rows[i + 1].FinancialYear);
//                                await UpdateFinancialYear(companyId, rows[i].Year, rows[i].Quarter, rows[i].FinancialYear, connection);
//                            }
//                        }
//                    }
//                }
//            }
//        }

//        private static async Task PropagateFinancialYear(int companyId, int year, int quarter, SqlConnection connection, int direction)
//        {
//            // Check if quarter is within bounds
//            if (quarter < 1 || quarter > 4)
//            {
//                if (quarter < 1) // If previous year should be checked
//                {
//                    year -= 1;
//                    quarter = 4;
//                }
//                else if (quarter > 4) // If next year should be checked
//                {
//                    year += 1;
//                    quarter = 1;
//                }
//            }

//            // Query to find adjacent quarter and check if FinancialYear is missing
//            string checkQuery = @"
//    SELECT FinancialYear, Year, Quarter
//    FROM FinancialData
//    WHERE CompanyID = @CompanyID AND ((Year = @Year AND Quarter = @Quarter) OR (Year = @PreviousYear AND Quarter = 4) OR (Year = @NextYear AND Quarter = 1))
//    ORDER BY Year, Quarter";  // Order by Year and Quarter for correctness

//            using (SqlCommand command = new SqlCommand(checkQuery, connection))
//            {
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", quarter);
//                command.Parameters.AddWithValue("@PreviousYear", year - 1);
//                command.Parameters.AddWithValue("@NextYear", year + 1);

//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    if (reader.Read())
//                    {
//                        int financialYear = reader.GetInt32(0);
//                        if (financialYear == 0) // FinancialYear is missing
//                        {
//                            Console.WriteLine($"[INFO] Filling missing FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter}");

//                            // Calculate FinancialYear for the missing quarter
//                            int calculatedYear = GetFinancialYearForReport(companyId, new DateTime(year, quarter * 3, 1), quarter, connection);

//                            if (calculatedYear > 0)
//                            {
//                                await UpdateFinancialYear(companyId, year, quarter, calculatedYear, connection);

//                                // Recursively propagate to the adjacent quarters in the same direction
//                                await PropagateFinancialYear(companyId, year, quarter + direction, connection, direction);
//                            }
//                        }
//                    }
//                }
//            }
//        }

//        private static async Task CheckAdjacentQuarters(int companyId, int year, int quarter, SqlConnection connection)
//        {
//            // Check previous quarter
//            await PropagateFinancialYear(companyId, year, quarter - 1, connection, -1);

//            // Check next quarter
//            await PropagateFinancialYear(companyId, year, quarter + 1, connection, 1);
//        }


//        private static async Task TryFillFinancialYear(int companyId, int year, int quarter, SqlConnection connection)
//        {
//            // Check if the financial year for the quarter is missing (NULL)
//            string checkQuery = @"
//    SELECT FinancialYear, EndDate
//    FROM FinancialData
//    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//            using (SqlCommand checkCommand = new SqlCommand(checkQuery, connection))
//            {
//                checkCommand.Parameters.AddWithValue("@CompanyID", companyId);
//                checkCommand.Parameters.AddWithValue("@Year", year);
//                checkCommand.Parameters.AddWithValue("@Quarter", quarter);

//                using (SqlDataReader reader = await checkCommand.ExecuteReaderAsync())
//                {
//                    if (await reader.ReadAsync())
//                    {
//                        var financialYear = reader.IsDBNull(0) ? (int?)null : reader.GetInt32(0);
//                        DateTime endDate = reader.GetDateTime(1);

//                        // If the financial year is missing, calculate and update it
//                        if (financialYear == null || financialYear == 0)
//                        {
//                            reader.Close();  // Make sure to close the reader before executing another command

//                            int newFinancialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);
//                            if (newFinancialYear > 0)
//                            {
//                                await UpdateFinancialYear(companyId, year, quarter, newFinancialYear, connection);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//        private static int GetFinancialYearForReport(int companyId, DateTime quarterEndDate, int quarter, SqlConnection connection)
//        {
//            // First, try to get the financial year from adjacent reports
//            int financialYear = GetFinancialYearFromAdjacentReports(companyId, quarterEndDate, connection, null);

//            // Fallback to fiscal year calculation if adjacent reports didn't yield a result
//            if (financialYear == 0)
//            {
//                financialYear = CalculateFinancialYear(companyId, quarterEndDate, connection, null);
//            }

//            // If both methods return 0, set the financialYear to 0
//            if (financialYear == 0)
//            {
//                Console.WriteLine($"[INFO] No financial year determined for CompanyID: {companyId}. Setting FinancialYear to 0.");
//                financialYear = 0;
//            }

//            return financialYear;
//        }
//        // Add this method to check adjacent quarters and fill missing FinancialYear values
//        private static async Task CheckAdjacentQuarters(int companyId, int year, int quarter, int financialYear, SqlConnection connection)
//        {
//            // Check previous quarter
//            if (quarter > 1)  // Only check if it's not the first quarter
//            {
//                int previousQuarter = quarter - 1;
//                int prevFinancialYear = GetFinancialYearForReport(companyId, year, previousQuarter, connection);

//                if (prevFinancialYear == 0)
//                {
//                    Console.WriteLine($"[INFO] Filling missing FinancialYear for previous quarter: CompanyID={companyId}, Year={year}, Quarter={previousQuarter}");
//                    prevFinancialYear = CalculateFinancialYear(companyId, new DateTime(year, (previousQuarter * 3), 1), connection, null);  // Recalculate based on adjacent report

//                    if (prevFinancialYear > 0)
//                    {
//                        await UpdateFinancialYear(companyId, year, previousQuarter, prevFinancialYear, connection);
//                    }
//                }
//            }

//            // Check next quarter
//            if (quarter < 4)  // Only check if it's not the last quarter
//            {
//                int nextQuarter = quarter + 1;
//                int nextFinancialYear = GetFinancialYearForReport(companyId, year, nextQuarter, connection);

//                if (nextFinancialYear == 0)
//                {
//                    Console.WriteLine($"[INFO] Filling missing FinancialYear for next quarter: CompanyID={companyId}, Year={year}, Quarter={nextQuarter}");
//                    nextFinancialYear = CalculateFinancialYear(companyId, new DateTime(year, (nextQuarter * 3), 1), connection, null);  // Recalculate based on adjacent report

//                    if (nextFinancialYear > 0)
//                    {
//                        await UpdateFinancialYear(companyId, year, nextQuarter, nextFinancialYear, connection);
//                    }
//                }
//            }
//        }

//        // Modify the existing GetFinancialYearForReport to handle adjacent reports effectively
//        private static int GetFinancialYearForReport(int companyId, int year, int quarter, SqlConnection connection)
//        {
//            DateTime quarterEndDate = new DateTime(year, (quarter * 3), 1).AddMonths(1).AddDays(-1);
//            int financialYear = GetFinancialYearFromAdjacentReports(companyId, quarterEndDate, connection, null);

//            // Fallback to fiscal year calculation if adjacent reports didn't yield a result
//            if (financialYear == 0)
//            {
//                financialYear = CalculateFinancialYear(companyId, quarterEndDate, connection, null);
//            }

//            // If both methods return 0, set the financialYear to 0
//            if (financialYear == 0)
//            {
//                Console.WriteLine($"[INFO] No financial year determined for CompanyID: {companyId}, Year: {year}, Quarter: {quarter}. Setting FinancialYear to 0.");
//                financialYear = 0;
//            }

//            return financialYear;
//        }


//        private static async Task ExecuteBatchedCommands(SqlConnection connection)
//        {
//            if (batchedCommands.Count > 0)
//            {
//                using (SqlTransaction transaction = connection.BeginTransaction())
//                {
//                    try
//                    {
//                        foreach (var command in batchedCommands)
//                        {
//                            command.Connection = connection;
//                            command.Transaction = transaction;
//                            await command.ExecuteNonQueryAsync();
//                        }

//                        transaction.Commit();
//                        Console.WriteLine("[INFO] Batch executed and committed.");
//                    }
//                    catch (Exception ex)
//                    {
//                        transaction.Rollback();
//                        Console.WriteLine($"[ERROR] Failed to execute batched commands: {ex.Message}");
//                    }
//                    finally
//                    {
//                        batchedCommands.Clear(); // Clear after execution
//                    }
//                }
//            }
//        }

//        private static async Task ScrapeAndProcessDataAsync()
//        {
//            try
//            {
//                Console.WriteLine("[INFO] Starting scraping and processing.");

//                // Process any unfinished rows before starting
//                await Data.Data.ProcessUnfinishedRows();
//                Console.WriteLine("[INFO] ProcessUnfinishedRows completed.");

//                // Get the list of companies from the database
//                var companies = await StockScraperV3.URL.GetNasdaq100CompaniesFromDatabase();
//                Console.WriteLine($"[INFO] Number of companies to process: {companies.Count}");

//                // Loop through each company and process their reports individually
//                foreach (var company in companies)
//                {
//                    Console.WriteLine($"[INFO] Starting processing for {company.companyName} ({company.symbol})");

//                    // Run the scraper for this company and process filings
//                    await StockScraperV3.URL.RunScraperAsync(); // Ensure this runs for each company

//                    // Log after scraping reports for this company is finished
//                    Console.WriteLine($"[INFO] Finished scraping reports for {company.companyName} ({company.symbol})");

//                    // Call CheckAndFillMissingFinancialYears for this company
//                    await semaphore.WaitAsync(); // Ensuring proper sequential execution
//                    try
//                    {
//                        Console.WriteLine($"[INFO] Calling CheckAndFillMissingFinancialYears for {company.companyName} ({company.symbol})");
//                        await CheckAndFillMissingFinancialYears(company.companyId);
//                        Console.WriteLine($"[INFO] Completed CheckAndFillMissingFinancialYears for {company.companyName} ({company.symbol})");
//                    }
//                    catch (Exception ex)
//                    {
//                        Console.WriteLine($"[ERROR] Exception while processing CheckAndFillMissingFinancialYears for {company.companyName}: {ex.Message}");
//                    }
//                    finally
//                    {
//                        semaphore.Release(); // Release the lock after finishing
//                    }
//                }

//                Console.WriteLine("[INFO] All companies' reports processed.");
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Scraping and processing failed: {ex.Message}");
//                throw; // Rethrow the exception for further handling
//            }
//            finally
//            {
//                Console.WriteLine("[INFO] ScrapeAndProcessDataAsync fully completed.");
//            }
//        }

//        // Method for filling missing financial years across all companies (post-scraping)
//        private static async Task CheckAndFillMissingFinancialYears()
//        {
//            Console.WriteLine("[INFO] Entered CheckAndFillMissingFinancialYears for all companies.");
//            wasCheckAndFillCalled = true; // Set the flag when the method is called

//            try
//            {
//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    Console.WriteLine("[INFO] Connection to database opened.");

//                    string query = @"
//                    SELECT CompanyID, Year, Quarter, EndDate
//                    FROM FinancialData
//                    WHERE FinancialYear IS NULL OR FinancialYear = 0";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                    {
//                        if (!reader.HasRows)
//                        {
//                            Console.WriteLine("[INFO] No rows with missing FinancialYear found.");
//                        }

//                        while (await reader.ReadAsync())
//                        {
//                            int companyId = reader.GetInt32(0);
//                            int year = reader.GetInt32(1);
//                            int quarter = reader.GetInt32(2);
//                            DateTime endDate = reader.GetDateTime(3);

//                            Console.WriteLine($"[INFO] Processing row: CompanyID={companyId}, Year={year}, Quarter={quarter}, EndDate={endDate}");

//                            int financialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);

//                            if (financialYear == 0)
//                            {
//                                Console.WriteLine("[INFO] No FinancialYear found, setting to 0.");
//                                financialYear = 0;
//                            }

//                            if (financialYear >= 0)
//                            {
//                                await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//                                Console.WriteLine($"[INFO] Updated FinancialYear to {financialYear} for CompanyID={companyId}");
//                            }
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine($"[INFO] CheckAndFillMissingFinancialYears was called: {wasCheckAndFillCalled}");
//            }
//        }

//        private static int CalculateFinancialYear(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            DateTime fiscalYearEnd = GetFiscalYearEndForSpecificYear(companyId, quarterEndDate.Year, connection, transaction);
//            DateTime fiscalYearStart = fiscalYearEnd.AddYears(-1).AddDays(1);

//            // Add more logging to check what fiscal year range is being used
//            Console.WriteLine($"[DEBUG] Fiscal Year Start: {fiscalYearStart}, Fiscal Year End: {fiscalYearEnd}, Quarter End Date: {quarterEndDate}");

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

//        private static int GetFinancialYearFromAdjacentReports(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            int financialYear = 0;

//            // Check next quarterly report
//            financialYear = GetFinancialYearFromNextQuarterlyReport(companyId, quarterEndDate, connection, transaction);

//            // If next quarterly report didn't help, check previous quarterly report
//            if (financialYear == 0)
//            {
//                financialYear = GetFinancialYearFromPreviousQuarterlyReport(companyId, quarterEndDate, connection, transaction);
//            }

//            return financialYear;
//        }
//        private static DateTime GetFiscalYearEndForSpecificYear(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//                    SELECT TOP 1 EndDate
//                    FROM FinancialData
//                    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 0
//                    ORDER BY EndDate DESC";

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
//        private static readonly SemaphoreSlim chromeSemaphore = new SemaphoreSlim(3, 3); // Semaphore for controlling ChromeDriver instances
//        private static async Task CallAfterScrapingAsync()
//        {
//            try
//            {
//                // Call after scraping to fill missing financial years for all companies
//                Console.WriteLine("[DEBUG] Calling CheckAndFillMissingFinancialYears() after scraping.");
//                await CheckAndFillMissingFinancialYears(); // No companyId needed here
//                Console.WriteLine("[DEBUG] Completed CheckAndFillMissingFinancialYears() after scraping.");
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Failed CheckAndFillMissingFinancialYears after scraping: {ex.Message}");
//            }
//        }

//        private static async Task FinalCheckAndFillAsync()
//        {
//            try
//            {
//                // Call again to ensure missing financial years are filled for all companies
//                Console.WriteLine("[DEBUG] Calling CheckAndFillMissingFinancialYears() finally.");
//                await CheckAndFillMissingFinancialYears(); // No companyId needed here
//                Console.WriteLine("[DEBUG] Completed CheckAndFillMissingFinancialYears() finally.");
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Failed final call to CheckAndFillMissingFinancialYears: {ex.Message}");
//            }
//        }

//        private static int GetFinancialYearFromPreviousQuarterlyReport(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                // Add detailed logging for debugging
//                Console.WriteLine($"[DEBUG] Running query for previous quarterly report. CompanyID: {companyId}, QuarterEndDate: {quarterEndDate}");

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND EndDate < @QuarterEndDate AND Quarter > 0
//        ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@QuarterEndDate", quarterEndDate);

//                var result = command.ExecuteScalar();

//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    Console.WriteLine($"[DEBUG] Found previous quarterly report. FinancialYear: {financialYear}, CompanyID: {companyId}, EndDate: {quarterEndDate}");
//                    return financialYear;
//                }
//                else
//                {
//                    Console.WriteLine($"[DEBUG] No previous quarterly report found for CompanyID: {companyId}, before {quarterEndDate}");
//                    return 0;
//                }
//            }
//        }
//        private static int GetFinancialYearFromNextQuarterlyReport(int companyId, DateTime currentEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                // Add detailed logging for debugging
//                Console.WriteLine($"[DEBUG] Running query for next quarterly report. CompanyID: {companyId}, CurrentEndDate: {currentEndDate}");

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND EndDate > DATEADD(day, -10, @CurrentEndDate) AND Quarter > 0
//        ORDER BY EndDate ASC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@CurrentEndDate", currentEndDate);

//                var result = command.ExecuteScalar();

//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    Console.WriteLine($"[DEBUG] Found next quarterly report. FinancialYear: {financialYear}, CompanyID: {companyId}, EndDate: {currentEndDate}");
//                    return financialYear;
//                }
//                else
//                {
//                    Console.WriteLine($"[DEBUG] No next quarterly report found for CompanyID: {companyId}, after {currentEndDate}");
//                    return 0;
//                }
//            }
//        }
//        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                Console.WriteLine($"[DEBUG] SaveQuarterData called. CompanyID: {companyId}, Quarter: {quarter}, ElementName: {elementName}");

//                // Try to get the financial year from adjacent reports
//                int financialYear = GetFinancialYearFromAdjacentReports(companyId, endDate, connection, transaction);

//                // If no financial year was found, fall back to fiscal year calculation
//                if (financialYear == 0)
//                {
//                    Console.WriteLine($"[WARNING] Could not fetch financial year from adjacent reports for CompanyID: {companyId}, Quarter: {quarter}. Falling back to fiscal year calculation.");
//                    financialYear = CalculateFinancialYear(companyId, endDate, connection, transaction);
//                }

//                // Ensure financialYear is set to 0 if not determined
//                if (financialYear == 0)
//                {
//                    Console.WriteLine($"[INFO] No financial year determined for CompanyID: {companyId}. Setting FinancialYear to 0.");
//                }

//                DateTime startDate = quarter == 0 ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);

//                Console.WriteLine($"[INFO] Handling quarterly report for company {companyId}. Financial year set to {financialYear}");

//                // Fetch or create the PeriodID from the Periods table
//                command.CommandText = @"
//            SELECT TOP 1 PeriodID 
//            FROM Periods 
//            WHERE Year = @Year AND Quarter = @Quarter";
//                command.Parameters.AddWithValue("@Year", financialYear);
//                command.Parameters.AddWithValue("@Quarter", quarter);

//                object result = command.ExecuteScalar();
//                if (result == null)
//                {
//                    Console.WriteLine($"[ERROR] Could not find corresponding period for CompanyID: {companyId}, Quarter: {quarter}, Financial Year: {financialYear}");
//                    return;
//                }

//                int periodId = Convert.ToInt32(result);

//                // Prepare insert or update command
//                command.Parameters.Clear();
//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@PeriodID", periodId);
//                command.Parameters.AddWithValue("@FinancialYear", financialYear); // This ensures 0 is saved if financialYear is 0
//                command.Parameters.AddWithValue("@Year", endDate.Year);
//                command.Parameters.AddWithValue("@Quarter", quarter);
//                command.Parameters.AddWithValue("@StartDate", startDate);
//                command.Parameters.AddWithValue("@EndDate", endDate);
//                command.Parameters.AddWithValue("@Value", value);

//                // Handle Annual or Quarterly reports
//                if (elementName == "AnnualReport")
//                {
//                    command.CommandText = @"
//                IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
//                BEGIN
//                    UPDATE FinancialData 
//                    SET FinancialYear = @FinancialYear, Year = @Year, Quarter = 0, StartDate = @StartDate, EndDate = @EndDate
//                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
//                END
//                ELSE
//                BEGIN
//                    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
//                    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, 0, @StartDate, @EndDate);
//                END";
//                }
//                else
//                {
//                    command.CommandText = @"
//                IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
//                BEGIN
//                    UPDATE FinancialData 
//                    SET FinancialYear = @FinancialYear, Year = @Year, Quarter = @Quarter, StartDate = @StartDate, EndDate = @EndDate 
//                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
//                END
//                ELSE
//                BEGIN
//                    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
//                    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate);
//                END";
//                }

//                // Add command to batch
//                batchedCommands.Add(command);
//                Console.WriteLine($"[INFO] Added command to batch for CompanyID: {companyId}, Quarter: {quarter}, FinancialYear: {financialYear}");
//            }
//        }
//        private static async Task FillMissingFinancialYearsAsync()
//        {
//            try
//            {
//                Console.WriteLine("[INFO] Starting to fill missing financial years.");

//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    string query = @"
//                SELECT CompanyID, Year, Quarter, EndDate
//                FROM FinancialData
//                WHERE FinancialYear IS NULL";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                    {
//                        while (await reader.ReadAsync())
//                        {
//                            int companyId = reader.GetInt32(0);
//                            int year = reader.GetInt32(1);
//                            int quarter = reader.GetInt32(2);
//                            DateTime endDate = reader.GetDateTime(3);

//                            Console.WriteLine($"[INFO] Processing CompanyID={companyId}, Year={year}, Quarter={quarter}, EndDate={endDate}");

//                            int financialYear = GetFinancialYearForReport(companyId, endDate, quarter, connection);

//                            // Explicitly set 0 if no financial year is determined
//                            if (financialYear == 0)
//                            {
//                                financialYear = 0;
//                                Console.WriteLine($"[INFO] FinancialYear is not determined. Setting to 0 for CompanyID={companyId}");
//                            }

//                            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//                        }
//                    }
//                }

//                Console.WriteLine("[INFO] Successfully filled missing financial years.");
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Error filling missing financial years: {ex.Message}");
//            }
//        }

//        private static async Task RecalculateMissingFinancialYears()
//        {
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();

//                // Query to find rows with missing financial years
//                string query = @"
//            SELECT CompanyID, Year, Quarter, EndDate
//            FROM FinancialData
//            WHERE FinancialYear IS NULL";

//                using (SqlCommand command = new SqlCommand(query, connection))
//                using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                {
//                    while (await reader.ReadAsync())
//                    {
//                        int companyId = reader.GetInt32(0);
//                        int year = reader.GetInt32(1);
//                        int quarter = reader.GetInt32(2);
//                        DateTime endDate = reader.GetDateTime(3);

//                        // Recalculate financial year based on company fiscal year
//                        int financialYear = CalculateFinancialYear(companyId, endDate, connection, null);

//                        // Update the financial year
//                        using (SqlCommand updateCommand = new SqlCommand(@"UPDATE FinancialData SET FinancialYear = @FinancialYear WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter", connection))
//                        {
//                            updateCommand.Parameters.AddWithValue("@FinancialYear", financialYear);
//                            updateCommand.Parameters.AddWithValue("@CompanyID", companyId);
//                            updateCommand.Parameters.AddWithValue("@Year", year);
//                            updateCommand.Parameters.AddWithValue("@Quarter", quarter);

//                            await updateCommand.ExecuteNonQueryAsync();
//                        }
//                    }
//                }
//            }
//        }
//        private static async Task MergeDuplicateRows()
//        {
//            using (SqlConnection connection = new SqlConnection(connectionString))
//            {
//                await connection.OpenAsync();
//                string findDuplicatesQuery = @"
//            SELECT CompanyID, StartDate, EndDate, COUNT(*) AS DuplicateCount
//            FROM FinancialData
//            GROUP BY CompanyID, StartDate, EndDate
//            HAVING COUNT(*) > 1";

//                using (SqlCommand findDuplicatesCommand = new SqlCommand(findDuplicatesQuery, connection))
//                using (SqlDataReader reader = await findDuplicatesCommand.ExecuteReaderAsync())
//                {
//                    while (await reader.ReadAsync())
//                    {
//                        int companyId = reader.GetInt32(0);
//                        DateTime startDate = reader.GetDateTime(1);
//                        DateTime endDate = reader.GetDateTime(2);

//                        // Step 2: Consolidate data for the duplicates
//                        await ConsolidateDuplicateRows(companyId, startDate, endDate, connection);
//                    }
//                }
//            }
//        }
//        private static async Task ConsolidateDuplicateRows(int companyId, DateTime startDate, DateTime endDate, SqlConnection connection)
//        {
//            string fetchDuplicatesQuery = @"
//        SELECT FinancialDataID, PeriodID, Value
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID AND StartDate = @StartDate AND EndDate = @EndDate";

//            using (SqlCommand fetchCommand = new SqlCommand(fetchDuplicatesQuery, connection))
//            {
//                fetchCommand.Parameters.AddWithValue("@CompanyID", companyId);
//                fetchCommand.Parameters.AddWithValue("@StartDate", startDate);
//                fetchCommand.Parameters.AddWithValue("@EndDate", endDate);

//                using (SqlDataReader reader = await fetchCommand.ExecuteReaderAsync())
//                {
//                    decimal consolidatedValue = 0;
//                    List<int> duplicateIds = new List<int>();

//                    while (await reader.ReadAsync())
//                    {
//                        int financialDataId = reader.GetInt32(reader.GetOrdinal("FinancialDataID"));
//                        decimal value = reader.GetDecimal(reader.GetOrdinal("Value"));
//                        consolidatedValue += value; // Aggregate values (can be customized based on your requirements)
//                        duplicateIds.Add(financialDataId); // Collect FinancialDataID for deletion
//                    }

//                    if (duplicateIds.Count > 0)
//                    {
//                        // Step 2b: Update one row with the consolidated value
//                        await UpdateConsolidatedRow(duplicateIds[0], consolidatedValue, connection);

//                        // Step 2c: Delete the duplicate rows except the one we updated
//                        await DeleteDuplicateRows(duplicateIds.Skip(1).ToList(), connection);
//                    }
//                }
//            }
//        }
//        private static async Task UpdateConsolidatedRow(int financialDataId, decimal consolidatedValue, SqlConnection connection)
//        {
//            string updateQuery = @"
//        UPDATE FinancialData
//        SET Value = @Value
//        WHERE FinancialDataID = @FinancialDataID";

//            using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection))
//            {
//                updateCommand.Parameters.AddWithValue("@FinancialDataID", financialDataId);
//                updateCommand.Parameters.AddWithValue("@Value", consolidatedValue);

//                await updateCommand.ExecuteNonQueryAsync();
//            }
//        }
//        private static async Task DeleteDuplicateRows(List<int> duplicateIds, SqlConnection connection)
//        {
//            if (duplicateIds.Count > 0)
//            {
//                string deleteQuery = @"
//            DELETE FROM FinancialData
//            WHERE FinancialDataID = @FinancialDataID";

//                foreach (int id in duplicateIds)
//                {
//                    using (SqlCommand deleteCommand = new SqlCommand(deleteQuery, connection))
//                    {
//                        deleteCommand.Parameters.AddWithValue("@FinancialDataID", id);
//                        await deleteCommand.ExecuteNonQueryAsync();
//                    }
//                }
//            }
//        }
//        private static int GetFinancialYearFromAnnualReport(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//                SELECT TOP 1 FinancialYear 
//                FROM FinancialData 
//                WHERE CompanyID = @CompanyID AND Quarter = 0
//                ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                object result = command.ExecuteScalar();
//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    return financialYear;
//                }
//                else
//                {
//                    return quarterEndDate.Year;
//                }
//            }
//        }
//        private static DateTime GetFiscalYearEnd(int companyId, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;
//                command.CommandText = @"
//        SELECT TOP 1 EndDate 
//        FROM FinancialData 
//        WHERE CompanyID = @CompanyID AND Quarter = 0
//        ORDER BY EndDate DESC";

//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                object result = command.ExecuteScalar();
//                if (result != null && DateTime.TryParse(result.ToString(), out DateTime fiscalYearEnd))
//                {
//                    Console.WriteLine($"[DEBUG] Fiscal Year End fetched from database for Company {companyId}: {fiscalYearEnd}");
//                    return fiscalYearEnd;
//                }
//                else
//                {
//                    Console.WriteLine($"[ERROR] No fiscal year end date found for Company {companyId}, defaulting to December 31 of current year.");
//                    return new DateTime(DateTime.Now.Year, 12, 31);
//                }
//            }
//        }
//        private static int GetFinancialYearFromNextQuarter(int companyId, int year, int nextQuarter, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", nextQuarter);

//                object result = command.ExecuteScalar();
//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    return financialYear;
//                }
//                return 0; // Return 0 if no financial year found for the next quarter
//            }
//        }
//        private static int GetFinancialYearFromPreviousQuarter(int companyId, int year, int previousQuarter, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                command.CommandText = @"
//        SELECT TOP 1 FinancialYear
//        FROM FinancialData
//        WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

//                command.Parameters.AddWithValue("@CompanyID", companyId);
//                command.Parameters.AddWithValue("@Year", year);
//                command.Parameters.AddWithValue("@Quarter", previousQuarter);

//                object result = command.ExecuteScalar();
//                if (result != null && int.TryParse(result.ToString(), out int financialYear))
//                {
//                    return financialYear;
//                }
//                return 0; // Return 0 if no financial year found for the previous quarter
//            }
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
//                    IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
//                    BEGIN
//                        INSERT INTO CompaniesList (CompanyName, CompanySymbol, CIK) 
//                        VALUES (@CompanyName, @CompanySymbol, @CIK);
//                    END";

//                        command.Parameters.AddWithValue("@CompanyName", company.CompanyName);
//                        command.Parameters.AddWithValue("@CompanySymbol", company.Ticker);
//                        if (int.TryParse(company.CIK, out int cikValue))
//                        {
//                            command.Parameters.AddWithValue("@CIK", cikValue);
//                        }
//                        else
//                        {
//                            Console.WriteLine($"Invalid CIK value for {company.CompanyName} ({company.Ticker}): {company.CIK}");
//                            continue; // Skip this record if CIK is not valid
//                        }

//                        await command.ExecuteNonQueryAsync();
//                    }
//                }
//            }
//        }
//    }
//}
