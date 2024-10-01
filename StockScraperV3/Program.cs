// TODO: THE YEAR COLUMN DOESN'T FIT IN SYNC WITH THE FINANCIAL YEAR, WHICH IT NEEDS TO FOR THE CALCULATION OF THE MISSING QUARTER 4 AND NON-CUMULATIVE CASH FLOWS.
// MISSING DATA POINTS IN THE XBRL AND HTML DATA.
// so code

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
                //Console.WriteLine("[INFO] Starting the program.");
                await ScrapeAndProcessDataAsync();  // Add your scraping logic in this method
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
            }
            finally
            {
                //Console.WriteLine("[INFO] Calling CheckAndFillMissingFinancialYears for all companies.");
                //await CheckAndFillMissingFinancialYears();
            }
        }

        public static int CalculateFinancialYear(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
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
                            //Console.WriteLine($"Invalid CIK value for {company.CompanyName} ({company.Ticker}): {company.CIK}");
                            continue; // Skip this record if CIK is not valid
                        }

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }

        //private static async Task CheckAndFillMissingFinancialYears()
        //{
        //    try
        //    {
        //        using (SqlConnection connection = new SqlConnection(connectionString))
        //        {
        //            await connection.OpenAsync();
        //            string query = @"
        //                SELECT CompanyID, Year, Quarter, EndDate
        //                FROM FinancialData
        //                WHERE FinancialYear IS NULL OR FinancialYear = 0";

        //            using (SqlCommand command = new SqlCommand(query, connection))
        //            using (SqlDataReader reader = await command.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {
        //                    int companyId = reader.GetInt32(0);
        //                    int year = reader.GetInt32(1);
        //                    int quarter = reader.GetInt32(2);
        //                    DateTime endDate = reader.GetDateTime(3);
        //                    int financialYear = GetFinancialYearForQuarter(quarter, endDate);

        //                    await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears: {ex.Message}");
        //    }
        //}

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
                //Console.WriteLine($"[INFO] Updated FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter} to {financialYear}");
            }
        }

        private static async Task ScrapeAndProcessDataAsync()
        {
            try
            {
                await Data.Data.ProcessUnfinishedRows();
                var companies = await StockScraperV3.URL.GetNasdaq100CompaniesFromDatabase();
                foreach (var company in companies)
                {
                    //Console.WriteLine($"[INFO] Starting processing for {company.companyName} ({company.symbol})");
                    await StockScraperV3.URL.RunScraperAsync(); // Ensure this runs for each company
                    await semaphore.WaitAsync(); // Ensuring proper sequential execution
                    try
                    {
                    }
                    catch (Exception ex)
                    {
                        //Console.WriteLine($"[ERROR] Exception while processing CheckAndFillMissingFinancialYears for {company.companyName}: {ex.Message}");
                    }
                    finally
                    {
                        semaphore.Release(); // Release the lock after finishing
                    }
                }
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"[ERROR] Scraping and processing failed: {ex.Message}");
                throw; // Rethrow the exception for further handling
            }
            finally
            {
            }
        }
    }
}

//using DataElements;
//using System.Data.SqlClient;
//using System.Threading;
//using System.Threading.Tasks;

//namespace Nasdaq100FinancialScraper
//{
//    class Program
//    {
//        public static readonly string connectionString = "Server=LAPTOP-871MLHAT\\sqlexpress;Database=StockDataScraperDatabase;Integrated Security=True;";
//        public static List<SqlCommand> batchedCommands = new List<SqlCommand>();
//        public static DateTime? globalStartDate = null;
//        public static DateTime? globalEndDate = null;
//        public static DateTime? globalInstantDate = null;
//        public static readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

//        static async Task Main(string[] args)
//        {
//            try
//            {
//                Console.WriteLine("[INFO] Starting the program.");
//                await ScrapeAndProcessDataAsync();  // Add your scraping logic in this method
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Scraping error: {ex.Message}");
//            }
//            finally
//            {
//                Console.WriteLine("[INFO] Calling CheckAndFillMissingFinancialYears for all companies.");
//                await CheckAndFillMissingFinancialYears();
//            }
//        }

//        private static int CalculateFinancialYear(int companyId, DateTime quarterEndDate, SqlConnection connection, SqlTransaction transaction)
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
//                            SELECT TOP 1 EndDate
//                            FROM FinancialData
//                            WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 0
//                            ORDER BY EndDate DESC";

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

//        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, SqlConnection connection, SqlTransaction transaction)
//        {
//            try
//            {
//                // Step 1: Log the input parameters for better traceability
//                Console.WriteLine($"[INFO] Processing data for CompanyID: {companyId}, Element: {elementName}, Quarter: {quarter}, EndDate: {endDate}");

//                using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
//                {
//                    // Step 2: Retrieve Company details (Name and Symbol)
//                    command.CommandText = @"SELECT CompanyName, CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
//                    command.Parameters.AddWithValue("@CompanyID", companyId);

//                    using (var reader = command.ExecuteReader())
//                    {
//                        if (!reader.Read())
//                        {
//                            Console.WriteLine($"[ERROR] No matching company found for CompanyID: {companyId}");
//                            return;
//                        }

//                        string companyName = reader["CompanyName"].ToString();
//                        string companySymbol = reader["CompanySymbol"].ToString();
//                        Console.WriteLine($"[DEBUG] Retrieved CompanyName: {companyName}, CompanySymbol: {companySymbol}");
//                        reader.Close();
//                    }

//                    // Step 3: Calculate the Financial Year
//                    int financialYear = (quarter == 0) ? endDate.Year : CalculateFinancialYear(companyId, endDate, connection, transaction);
//                    Console.WriteLine($"[DEBUG] Calculated FinancialYear: {financialYear}");

//                    if (financialYear == 0)
//                    {
//                        Console.WriteLine($"[WARNING] Could not determine financial year for CompanyID: {companyId}, Quarter: {quarter}. Setting to 0.");
//                    }

//                    // Step 4: Fetch or create the PeriodID
//                    command.CommandText = @"
//            DECLARE @ExistingPeriodID INT;
//            SET @ExistingPeriodID = (SELECT TOP 1 PeriodID FROM Periods WHERE Year = @Year AND Quarter = @Quarter);
//            IF @ExistingPeriodID IS NOT NULL
//            BEGIN
//                SELECT @ExistingPeriodID;
//            END
//            ELSE
//            BEGIN
//                INSERT INTO Periods (Year, Quarter) VALUES (@Year, @Quarter);
//                SELECT SCOPE_IDENTITY();
//            END";
//                    command.Parameters.Clear();
//                    command.Parameters.AddWithValue("@Year", financialYear);
//                    command.Parameters.AddWithValue("@Quarter", quarter);

//                    int periodId = Convert.ToInt32(command.ExecuteScalar());
//                    Console.WriteLine($"[DEBUG] Retrieved or created PeriodID: {periodId}");

//                    // Step 5: Calculate the start date based on whether it's an annual or quarterly report
//                    DateTime startDate = (quarter == 0) ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);
//                    Console.WriteLine($"[DEBUG] Calculated StartDate: {startDate} for CompanyID: {companyId}, Quarter: {quarter}");

//                    // Step 6: Retrieve existing data for the period (if it exists)
//                    command.CommandText = @"
//            SELECT * 
//            FROM FinancialData
//            WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";
//                    command.Parameters.Clear();
//                    command.Parameters.AddWithValue("@CompanyID", companyId);
//                    command.Parameters.AddWithValue("@PeriodID", periodId);

//                    bool hasExistingRow = false;
//                    bool isParsedFullHTMLNull = false;
//                    bool isParsedFullXBRLNull = false;

//                    using (var reader = command.ExecuteReader())
//                    {
//                        if (reader.Read())
//                        {
//                            hasExistingRow = true;
//                            isParsedFullHTMLNull = reader["ParsedFullHTML"] == DBNull.Value;
//                            isParsedFullXBRLNull = reader["ParsedFullXBRL"] == DBNull.Value;
//                            Console.WriteLine($"[DEBUG] Found existing row for CompanyID: {companyId}, PeriodID: {periodId}, ParsedFullHTML is null: {isParsedFullHTMLNull}");
//                        }
//                        reader.Close();
//                    }

//                    // Step 7: Prepare to either update or insert the row in FinancialData table
//                    if (hasExistingRow)
//                    {
//                        // Update existing row
//                        command.CommandText = @"
//                UPDATE FinancialData 
//                SET 
//                    FinancialYear = COALESCE(@FinancialYear, FinancialYear), 
//                    StartDate = COALESCE(@StartDate, StartDate), 
//                    EndDate = COALESCE(@EndDate, EndDate),
//                    ParsedFullHTML = CASE WHEN @ParsedFullHTML IS NOT NULL THEN @ParsedFullHTML ELSE ParsedFullHTML END,
//                    ParsedFullXBRL = CASE WHEN @ParsedFullXBRL IS NOT NULL THEN @ParsedFullXBRL ELSE ParsedFullXBRL END
//                WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";
//                        Console.WriteLine($"[DEBUG] Updating existing row for CompanyID: {companyId}, PeriodID: {periodId}");
//                    }
//                    else
//                    {
//                        // Insert new row
//                        command.CommandText = @"
//                INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate, ParsedFullHTML, ParsedFullXBRL)
//                VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate, @ParsedFullHTML, @ParsedFullXBRL)";
//                        Console.WriteLine($"[DEBUG] Inserting new row for CompanyID: {companyId}, PeriodID: {periodId}");
//                    }

//                    // Step 8: Bind the parameters for the INSERT/UPDATE query
//                    command.Parameters.Clear();
//                    command.Parameters.AddWithValue("@CompanyID", companyId);
//                    command.Parameters.AddWithValue("@PeriodID", periodId);
//                    command.Parameters.AddWithValue("@FinancialYear", financialYear);
//                    command.Parameters.AddWithValue("@Year", endDate.Year);
//                    command.Parameters.AddWithValue("@Quarter", quarter);
//                    command.Parameters.AddWithValue("@StartDate", startDate);
//                    command.Parameters.AddWithValue("@EndDate", endDate);
//                    command.Parameters.AddWithValue("@ParsedFullHTML", DBNull.Value);  // Set to DBNull if no update required
//                    command.Parameters.AddWithValue("@ParsedFullXBRL", DBNull.Value);  // Set to DBNull if no update required

//                    // Step 9: Execute the query and log the number of affected rows
//                    int rowsAffected = command.ExecuteNonQuery();
//                    Console.WriteLine($"[INFO] {rowsAffected} row(s) affected for CompanyID: {companyId}, PeriodID: {periodId}, Quarter: {quarter}, FinancialYear: {financialYear}");

//                    // Step 10: Handle additional element-specific updates (if applicable)
//                    if (elementName != "AnnualReport" && elementName != "QuarterlyReport")
//                    {
//                        string columnName = GetColumnName(elementName);

//                        if (!string.IsNullOrEmpty(columnName))
//                        {
//                            command.CommandText = $@"
//                    UPDATE FinancialData 
//                    SET [{columnName}] = @Value
//                    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID";
//                            command.Parameters.Clear();
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@PeriodID", periodId);
//                            command.Parameters.AddWithValue("@Value", value);

//                            int elementRowsAffected = command.ExecuteNonQuery();
//                            Console.WriteLine($"[INFO] {elementRowsAffected} row(s) affected for CompanyID: {companyId}, Quarter: {quarter}, Element: {elementName}");
//                        }
//                        else
//                        {
//                            Console.WriteLine($"[WARNING] No matching column found for element name: {elementName}");
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception while saving data for CompanyID: {companyId}, Element: {elementName}. Exception: {ex.Message}");
//                throw;  // Rethrow the exception to ensure proper handling in the calling method
//            }
//        }

//        private static string GetColumnName(string elementName)
//        {
//            // Check if the element is part of the InstantDateElements or ElementsOfInterest sets (XBRL parsing)
//            if (FinancialElementLists.InstantDateElements.Contains(elementName) || FinancialElementLists.ElementsOfInterest.Contains(elementName))
//            {
//                return elementName; // The element name itself is the column name in these cases
//            }

//            // Check if the element is part of HTMLElementsOfInterest (HTML parsing)
//            if (FinancialElementLists.HTMLElementsOfInterest.TryGetValue(elementName, out var htmlElement))
//            {
//                return htmlElement.ColumnName; // Get the corresponding column name from the HTML elements dictionary
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
//                            Console.WriteLine($"Invalid CIK value for {company.CompanyName} ({company.Ticker}): {company.CIK}");
//                            continue; // Skip this record if CIK is not valid
//                        }

//                        await command.ExecuteNonQueryAsync();
//                    }
//                }
//            }
//        }

//        private static async Task CheckAndFillMissingFinancialYears()
//        {
//            Console.WriteLine("[INFO] Entered CheckAndFillMissingFinancialYears for all companies.");

//            try
//            {
//                using (SqlConnection connection = new SqlConnection(connectionString))
//                {
//                    await connection.OpenAsync();
//                    Console.WriteLine("[INFO] Connection to database opened.");

//                    string query = @"
//                        SELECT CompanyID, Year, Quarter, EndDate
//                        FROM FinancialData
//                        WHERE FinancialYear IS NULL OR FinancialYear = 0";

//                    using (SqlCommand command = new SqlCommand(query, connection))
//                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
//                    {
//                        while (await reader.ReadAsync())
//                        {
//                            int companyId = reader.GetInt32(0);
//                            int year = reader.GetInt32(1);
//                            int quarter = reader.GetInt32(2);
//                            DateTime endDate = reader.GetDateTime(3);

//                            Console.WriteLine($"[INFO] Processing row: CompanyID={companyId}, Year={year}, Quarter={quarter}, EndDate={endDate}");

//                            int financialYear = GetFinancialYearForQuarter(quarter, endDate);

//                            await UpdateFinancialYear(companyId, year, quarter, financialYear, connection);
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                Console.WriteLine($"[ERROR] Exception in CheckAndFillMissingFinancialYears: {ex.Message}");
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
//                Console.WriteLine($"[INFO] Updated FinancialYear for CompanyID={companyId}, Year={year}, Quarter={quarter} to {financialYear}");
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
//    }
//}