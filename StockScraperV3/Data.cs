using DataElements;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static DataElements.FinancialElementLists;
using System.Xml.Linq;
using Newtonsoft.Json;

namespace Data
{
    public class Data
    {
        public static void CalculateAndSaveQ4InDatabase(SqlConnection connection, int companyId)
        {
            Console.WriteLine($"[INFO] Starting Q4 calculation for CompanyID: {companyId}");

            // Fetch all years where Quarter = 0 (annual reports)
            string fetchYearsQuery = @"
    SELECT DISTINCT Year, EndDate
    FROM FinancialData
    WHERE CompanyID = @CompanyID AND Quarter = 0;";

            List<(int year, DateTime endDate)> annualReports = new List<(int, DateTime)>();

            using (SqlCommand command = new SqlCommand(fetchYearsQuery, connection))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        annualReports.Add((reader.GetInt32(0), reader.GetDateTime(1)));
                    }
                }
            }

            foreach (var (year, endDate) in annualReports)
            {
                Console.WriteLine($"[INFO] Calculating Q4 for Year: {year}");

                foreach (var element in FinancialElementLists.HTMLElementsOfInterest)
                {
                    string columnName = element.Value.ColumnName;
                    bool isShares = element.Value.IsShares;
                    bool isCashFlowStatement = element.Value.IsCashFlowStatement;
                    bool isBalanceSheet = element.Value.IsBalanceSheet;

                    try
                    {
                        // Build the dynamic SQL update statement for Q4 calculation
                        string q4CalculationQuery = $@"
    UPDATE FD4
    SET FD4.[{columnName}] = 
        CASE 
            WHEN @IsShares = 1 THEN FD1.[{columnName}] -- Use annual value for shares
            WHEN @IsCashFlowStatement = 1 THEN FD1.[{columnName}] - COALESCE(FD3.[{columnName}], 0) -- Annual minus Q3 for cash flow
            WHEN @IsBalanceSheet = 1 THEN FD1.[{columnName}] -- Use annual value for balance sheet elements
            ELSE FD1.[{columnName}] - COALESCE(FD2.[{columnName}], 0) - COALESCE(FD3.[{columnName}], 0) - COALESCE(FD4.[{columnName}], 0) -- Standard Q4 calculation
        END,
        FD4.FinancialYear = FD1.FinancialYear -- Set FinancialYear from the annual report
    FROM FinancialData FD1
    LEFT JOIN FinancialData FD2 ON FD1.CompanyID = FD2.CompanyID AND FD1.Year = FD2.Year AND FD2.Quarter = 1
    LEFT JOIN FinancialData FD3 ON FD1.CompanyID = FD3.CompanyID AND FD1.Year = FD3.Year AND FD3.Quarter = 2
    LEFT JOIN FinancialData FD4 ON FD1.CompanyID = FD4.CompanyID AND FD1.Year = FD4.Year AND FD4.Quarter = 4
    WHERE FD1.Quarter = 0 AND FD1.CompanyID = @CompanyID AND FD1.Year = @Year
    AND ABS(DATEDIFF(day, FD1.EndDate, FD4.EndDate)) <= @LeewayDays;";  // Leeway for matching end dates

                        using (SqlCommand command = new SqlCommand(q4CalculationQuery, connection))
                        {
                            command.Parameters.AddWithValue("@CompanyID", companyId);
                            command.Parameters.AddWithValue("@Year", year);
                            command.Parameters.AddWithValue("@IsShares", isShares ? 1 : 0);
                            command.Parameters.AddWithValue("@IsCashFlowStatement", isCashFlowStatement ? 1 : 0);
                            command.Parameters.AddWithValue("@IsBalanceSheet", isBalanceSheet ? 1 : 0);
                            command.Parameters.AddWithValue("@LeewayDays", 5);  // Allow 5 days leeway for matching dates

                            int rowsAffected = command.ExecuteNonQuery();

                            Console.WriteLine($"[INFO] Q4 calculation for element: {columnName} complete. Rows affected: {rowsAffected} for CompanyID: {companyId}, Year: {year}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Failed to update Q4 for element: {columnName}, CompanyID: {companyId}, Year: {year}. Error: {ex.Message}");
                    }
                }

                // Update ParsedFullHTML and ParsedFullXBRL fields after the Q4 calculation
                string updateParsedFieldsQuery = @"
    UPDATE FinancialData 
    SET ParsedFullHTML = CASE WHEN @ParsedFullHTML = 'Yes' THEN 'Yes' ELSE ParsedFullHTML END,
        ParsedFullXBRL = CASE WHEN @ParsedFullXBRL = 'Yes' THEN 'Yes' ELSE ParsedFullXBRL END
    WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = 4;";

                using (SqlCommand command = new SqlCommand(updateParsedFieldsQuery, connection))
                {
                    command.Parameters.AddWithValue("@CompanyID", companyId);
                    command.Parameters.AddWithValue("@Year", year);
                    command.Parameters.AddWithValue("@ParsedFullHTML", "Yes");
                    command.Parameters.AddWithValue("@ParsedFullXBRL", "Yes");

                    int parsedRowsAffected = command.ExecuteNonQuery();
                    Console.WriteLine($"[INFO] ParsedFullHTML and ParsedFullXBRL updated for Q4. Rows affected: {parsedRowsAffected} for CompanyID: {companyId}, Year: {year}");
                }
            }
        }



        public static void CalculateQ4InDatabase(SqlConnection connection, int companyId)
        {
            // Log before calculation to verify we are entering the method
            //Console.WriteLine($"[INFO] Starting Q4 calculation for CompanyID: {companyId}");

            // Iterate through each element in the FinancialElementLists.ElementsOfInterest
            foreach (var element in FinancialElementLists.ElementsOfInterest)
            {
                try
                {
                    // Build the dynamic SQL update statement for Q4 calculation
                    string q4CalculationQuery = $@"
            UPDATE FD4
            SET FD4.[{element}] = 
                (FD1.[{element}] - COALESCE(FD2.[{element}], 0) - COALESCE(FD3.[{element}], 0))
            FROM FinancialData FD1
            LEFT JOIN FinancialData FD2 ON FD1.CompanyID = FD2.CompanyID AND FD1.Year = FD2.Year AND FD2.Quarter = 1
            LEFT JOIN FinancialData FD3 ON FD1.CompanyID = FD3.CompanyID AND FD1.Year = FD3.Year AND FD3.Quarter = 2
            LEFT JOIN FinancialData FD4 ON FD1.CompanyID = FD4.CompanyID AND FD1.Year = FD4.Year AND FD4.Quarter = 4
            WHERE FD1.Quarter = 0 AND FD1.CompanyID = @CompanyID;";

                    // Log the constructed SQL query for debugging purposes
                    //Console.WriteLine($"[DEBUG] Executing Q4 calculation for element: {element}");

                    using (SqlCommand command = new SqlCommand(q4CalculationQuery, connection))
                    {
                        // Add the parameter for CompanyID
                        command.Parameters.AddWithValue("@CompanyID", companyId);

                        // Execute the query
                        int rowsAffected = command.ExecuteNonQuery();

                        // Log how many rows were affected
                        //Console.WriteLine($"[INFO] Q4 calculation for element: {element} complete. Rows affected: {rowsAffected} for CompanyID: {companyId}");
                    }
                }
                catch (Exception ex)
                {
                    // Log any errors that occur during the execution of each individual column
                    //Console.WriteLine($"[ERROR] Failed to update Q4 for element: {element}, CompanyID: {companyId}. Error: {ex.Message}");
                }
            }
        }

        private static void CheckAndFillMissingFinancialYears(int companyId, int year, int quarter)
        {
            if (quarter == 0)
            {
                // Log that we're handling the annual report
                Console.WriteLine($"[INFO] Handling annual report for CompanyID: {companyId}, Year: {year} (Quarter = 0)");

                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
                {
                    connection.Open();
                    using (SqlTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            // Step 1: Extract the EndDate year and insert it into the FinancialYear column
                            string updateFinancialYearQuery = @"
                    UPDATE FD
                    SET FD.FinancialYear = YEAR(FD.EndDate)
                    FROM FinancialData FD
                    WHERE FD.CompanyID = @CompanyID AND FD.Quarter = 0 AND FD.FinancialYear IS NULL";

                            using (var command = new SqlCommand(updateFinancialYearQuery, connection, transaction))
                            {
                                command.Parameters.AddWithValue("@CompanyID", companyId);

                                int rowsAffected = command.ExecuteNonQuery();

                                // Log the result
                                Console.WriteLine($"[INFO] Updated FinancialYear for {rowsAffected} annual reports for CompanyID: {companyId}.");
                            }

                            // Commit transaction
                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Failed to update FinancialYear for CompanyID: {companyId}. Error: {ex.Message}");
                            transaction.Rollback();
                        }
                    }
                }
                return;  // Exit as no further processing is needed for annual reports
            }

            // Logic for quarters 1, 2, 3, and 4
            if (quarter < 1 || quarter > 4)
            {
                throw new ArgumentException($"Invalid quarter value: {quarter}");
            }

            // Add logic to fill missing financial years for a company for a specific quarter.
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                connection.Open();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        // Example: Logic to fill in missing financial year data for the specified quarter
                        string fillMissingDataQuery = @"
                UPDATE FinancialData
                SET FinancialYear = @Year
                WHERE CompanyID = @CompanyID AND Quarter = @Quarter AND Year IS NULL";

                        using (var command = new SqlCommand(fillMissingDataQuery, connection, transaction))
                        {
                            command.Parameters.AddWithValue("@CompanyID", companyId);
                            command.Parameters.AddWithValue("@Year", year);
                            command.Parameters.AddWithValue("@Quarter", quarter);

                            int rowsAffected = command.ExecuteNonQuery();
                            Console.WriteLine($"[INFO] Filled missing financial year for {rowsAffected} records. CompanyID: {companyId}, Year: {year}, Quarter: {quarter}");
                        }

                        // Commit the transaction
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Failed to fill missing financial year data. Error: {ex.Message}");
                        transaction.Rollback();
                    }
                }
            }
        }
        public static decimal? GetQuarterData(int companyId, int year, int quarter, string elementName, SqlConnection connection, SqlTransaction transaction)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandText = $@"
            SELECT TOP 1 [{elementName}]
            FROM FinancialData
            WHERE CompanyID = @CompanyID AND Year = @Year AND Quarter = @Quarter";

                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Year", year);
                command.Parameters.AddWithValue("@Quarter", quarter);

                object result = command.ExecuteScalar();

                if (result != null && decimal.TryParse(result.ToString(), out decimal quarterValue))
                {
                    return quarterValue;
                }
                else
                {
                    return null;
                }
            }
        }
        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, bool isHtmlParsed, bool isXbrlParsed, int leewayDays = 5)
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                connection.Open();
                SqlTransaction transaction = connection.BeginTransaction();

                try
                {
                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
                    {
                        // Step 1: Retrieve Company details (Name and Symbol)
                        command.CommandText = @"SELECT CompanyName, CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
                        command.Parameters.AddWithValue("@CompanyID", companyId);

                        using (var reader = command.ExecuteReader())
                        {
                            if (!reader.Read())
                            {
                                Console.WriteLine($"[ERROR] No matching company found for CompanyID: {companyId}");
                                return;
                            }
                            reader.Close();
                        }

                        int financialYearStartMonth = GetCompanyFiscalStartMonth(companyId, connection, transaction);

                        // Step 2: Calculate Quarter
                        if (quarter != 0)
                        {
                            quarter = CalculateQuarterBasedOnFiscalYear(endDate, financialYearStartMonth);
                        }

                        // Step 3: Calculate Financial Year
                        int financialYear = (quarter == 0) ? endDate.Year : Nasdaq100FinancialScraper.Program.CalculateFinancialYear(companyId, endDate, connection, transaction);

                        // Step 4: Fetch or create the PeriodID
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

                        // Step 5: Calculate the start date for annual or quarterly reports
                        DateTime startDate = (quarter == 0) ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);

                        // Step 6: Check for existing rows with the same CompanyID, StartDate, and EndDate (with leeway)
                        command.CommandText = @"
SELECT COUNT(*) 
FROM FinancialData 
WHERE CompanyID = @CompanyID 
AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        command.Parameters.AddWithValue("@StartDate", startDate);
                        command.Parameters.AddWithValue("@EndDate", endDate);
                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);

                        int existingRowCount = (int)command.ExecuteScalar();
                        bool hasExistingRow = existingRowCount > 0;

                        // Step 7: Update or insert the row in the FinancialData table (excluding HTML/XBRL)
                        if (hasExistingRow)
                        {
                            command.CommandText = @"
    UPDATE FinancialData 
    SET 
        FinancialYear = COALESCE(@FinancialYear, FinancialYear), 
        StartDate = COALESCE(@StartDate, StartDate), 
        EndDate = COALESCE(@EndDate, EndDate)
    WHERE CompanyID = @CompanyID 
    AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
    AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
                        }
                        else
                        {
                            command.CommandText = @"
    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate)";
                        }

                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        command.Parameters.AddWithValue("@PeriodID", periodId);
                        command.Parameters.AddWithValue("@FinancialYear", financialYear);
                        command.Parameters.AddWithValue("@Year", endDate.Year);
                        command.Parameters.AddWithValue("@Quarter", quarter);
                        command.Parameters.AddWithValue("@StartDate", startDate);
                        command.Parameters.AddWithValue("@EndDate", endDate);
                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);

                        Nasdaq100FinancialScraper.Program.batchedCommands.Add(CloneCommand(command));

                        // Step 8: Handle additional element-specific updates (if applicable)
                        if (elementName != "AnnualReport" && elementName != "QuarterlyReport")
                        {
                            string columnName = Nasdaq100FinancialScraper.Program.GetColumnName(elementName);
                            if (!string.IsNullOrEmpty(columnName))
                            {
                                command.CommandText = $@"
        UPDATE FinancialData 
        SET [{columnName}] = @Value
        WHERE CompanyID = @CompanyID 
        AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
        AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@CompanyID", companyId);
                                command.Parameters.AddWithValue("@StartDate", startDate);
                                command.Parameters.AddWithValue("@EndDate", endDate);
                                command.Parameters.AddWithValue("@Value", value);
                                command.Parameters.AddWithValue("@LeewayDays", leewayDays);

                                Nasdaq100FinancialScraper.Program.batchedCommands.Add(CloneCommand(command));
                            }
                        }

                        // Step 9: Update ParsedFullHTML and ParsedFullXBRL status only if parsing is completed
                        if (isHtmlParsed || isXbrlParsed)
                        {
                            command.CommandText = @"
    UPDATE FinancialData 
    SET ParsedFullHTML = CASE WHEN @ParsedFullHTML = 'Yes' THEN 'Yes' ELSE ParsedFullHTML END,
        ParsedFullXBRL = CASE WHEN @ParsedFullXBRL = 'Yes' THEN 'Yes' ELSE ParsedFullXBRL END
    WHERE CompanyID = @CompanyID 
    AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
    AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@CompanyID", companyId);
                            command.Parameters.AddWithValue("@StartDate", startDate);
                            command.Parameters.AddWithValue("@EndDate", endDate);
                            command.Parameters.AddWithValue("@ParsedFullHTML", isHtmlParsed ? "Yes" : DBNull.Value);
                            command.Parameters.AddWithValue("@ParsedFullXBRL", isXbrlParsed ? "Yes" : DBNull.Value);
                            command.Parameters.AddWithValue("@LeewayDays", leewayDays);

                            Nasdaq100FinancialScraper.Program.batchedCommands.Add(CloneCommand(command));
                        }
                    }

                    // Step 10: Mark the quarter as parsed and check for Q4 calculation
                    MarkQuarterAsParsed(companyId, endDate.Year, quarter);  // Track the parsed quarter
                    Console.WriteLine($"[DEBUG] Quarter {quarter} parsed for CompanyID: {companyId}, Year: {endDate.Year}");

                    // Check if all three quarters (Q1, Q2, and Q3) have been parsed
                    if (AreAllQuartersParsed(companyId, endDate.Year))
                    {
                        Console.WriteLine($"[INFO] All quarters parsed for CompanyID: {companyId}, Year: {endDate.Year}. Triggering Q4 calculation.");
                        //CalculateMissingQuarter(companyId, endDate.Year).Wait();
                    }

                    transaction.Commit();  // Commit the transaction
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine($"[ERROR] Transaction rolled back due to error: {ex.Message}");
                    throw;
                }
            }
        }
        private static void MarkQuarterAsParsed(int companyId, int year, int quarter)
        {
            if (!parsedQuarterTracker.ContainsKey(companyId))
            {
                parsedQuarterTracker[companyId] = new Dictionary<int, HashSet<int>>();
            }

            if (!parsedQuarterTracker[companyId].ContainsKey(year))
            {
                parsedQuarterTracker[companyId][year] = new HashSet<int>();
            }

            // Mark the quarter as parsed
            parsedQuarterTracker[companyId][year].Add(quarter);
        }
        private static bool AreAllQuartersParsed(int companyId, int year)
        {
            return parsedQuarterTracker.ContainsKey(companyId) &&
                   parsedQuarterTracker[companyId].ContainsKey(year) &&
                   parsedQuarterTracker[companyId][year].Contains(1) &&
                   parsedQuarterTracker[companyId][year].Contains(2) &&
                   parsedQuarterTracker[companyId][year].Contains(3);
        }
        private static int GetCompanyFiscalStartMonth(int companyId, SqlConnection connection, SqlTransaction transaction)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.Transaction = transaction;

                // Try to get the fiscal start month from the FinancialData table by finding the earliest entry
                command.CommandText = @"
            SELECT TOP 1 MONTH(StartDate) 
            FROM FinancialData 
            WHERE CompanyID = @CompanyID 
            ORDER BY StartDate ASC";
                command.Parameters.AddWithValue("@CompanyID", companyId);

                object result = command.ExecuteScalar();
                if (result != null)
                {
                    // Return the month as the fiscal start month
                    return (int)result;
                }
                else
                {
                    // Default to January if no data is found
                    return 1;
                }
            }
        }

        public static int GetQuarterFromEndDate(DateTime endDate, int companyId, SqlConnection connection, SqlTransaction transaction)
        {
            // Fetch fiscal start month using the earliest start date, now with the transaction parameter
            int fiscalStartMonth = GetCompanyFiscalStartMonth(companyId, connection, transaction);

            // Map end date to the correct quarter based on the fiscal year start month
            if (fiscalStartMonth == 1) // Fiscal year starts in January
            {
                if (endDate.Month <= 3) return 1;
                else if (endDate.Month <= 6) return 2;
                else if (endDate.Month <= 9) return 3;
                else return 4;
            }
            else
            {
                // Adjust quarter calculation based on non-standard fiscal year starts
                int fiscalMonthOffset = (endDate.Month - fiscalStartMonth + 12) % 12;
                if (fiscalMonthOffset < 3) return 1;
                else if (fiscalMonthOffset < 6) return 2;
                else if (fiscalMonthOffset < 9) return 3;
                else return 4;
            }
        }
        public static int CalculateQuarterBasedOnFiscalYear(DateTime endDate, int fiscalStartMonth, int leewayDays = 14)
        {
            // Subtract exactly 364 days from the end date to determine the fiscal year start
            DateTime fiscalYearStart = endDate.AddDays(-364);

            // Apply leeway for fiscal year shifts based on the reporting cycle
            DateTime fiscalYearStartWithLeeway = fiscalYearStart.AddDays(-leewayDays);
            if (endDate < fiscalYearStartWithLeeway)
            {
                fiscalYearStart = fiscalYearStart.AddYears(-1);
            }
            int monthInFiscalYear = ((endDate.Month - fiscalYearStart.Month + 12) % 12) + 1;
            if (monthInFiscalYear <= 3) return 1;
            if (monthInFiscalYear <= 6) return 2;
            if (monthInFiscalYear <= 9) return 3;
            return 4;
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
        public static async Task ProcessUnfinishedRows()
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                await connection.OpenAsync();
                string query = @"
                    SELECT CompanyID, PeriodID, Year, Quarter, ParsedFullHTML, ParsedFullXBRL 
                    FROM FinancialData 
                    WHERE ParsedFullHTML IS NULL OR ParsedFullXBRL IS NULL 
                    OR ParsedFullHTML != 'Yes' OR ParsedFullXBRL != 'Yes'";
                using (SqlCommand command = new SqlCommand(query, connection))
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    var tasks = new List<Task>();
                    while (await reader.ReadAsync())
                    {
                        int companyId = reader.GetInt32(0);
                        int periodId = reader.GetInt32(1);
                        int year = reader.GetInt32(2);
                        int quarter = reader.GetInt32(3);
                        bool needsHtmlParsing = reader.IsDBNull(4) || reader.GetString(4) != "Yes";
                        bool needsXbrlParsing = reader.IsDBNull(5) || reader.GetString(5) != "Yes";
                        string companyName = await StockScraperV3.URL.GetCompanyName(companyId);
                        string companySymbol = await StockScraperV3.URL.GetCompanySymbol(companyId);
                        tasks.Add(Task.Run(async () =>
                        {
                            if (needsHtmlParsing)
                            {
                                await HTML.HTML.ReparseHtmlReports(companyId, periodId, year, quarter, companyName, companySymbol);
                            }
                            if (needsXbrlParsing)
                            {
                                await XBRL.XBRL.ReparseXbrlReports(companyId, periodId, year, quarter, companyName, companySymbol);
                            }
                        }));
                    }

                    await Task.WhenAll(tasks);
                }
            }
            await ExecuteBatch();
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

        public static void SaveToDatabase(string elementName, string value, XElement? context, string[] elementsOfInterest, List<FinancialElement> elements, bool isAnnualReport, string companyName, string companySymbol, bool isHtmlParsed = false, bool isXbrlParsed = false)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
                {
                    connection.Open();
                    using (var transaction = connection.BeginTransaction())
                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
                    {
                        try
                        {
                            command.CommandText = @"
IF NOT EXISTS (SELECT 1 FROM CompaniesList WHERE CompanySymbol = @CompanySymbol)
BEGIN
    INSERT INTO CompaniesList (CompanyName, CompanySymbol, CompanyStockExchange, Industry, Sector)
    VALUES (@CompanyName, @CompanySymbol, @CompanyStockExchange, @Industry, @Sector);
END";
                            command.Parameters.AddWithValue("@CompanyName", companyName);
                            command.Parameters.AddWithValue("@CompanySymbol", companySymbol);
                            command.Parameters.AddWithValue("@CompanyStockExchange", "NASDAQ");
                            command.Parameters.AddWithValue("@Industry", "Technology");
                            command.Parameters.AddWithValue("@Sector", "Consumer Electronics");
                            command.ExecuteNonQuery();

                            // Step 2: Get the CompanyID
                            command.CommandText = "SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
                            int companyId = (int)command.ExecuteScalar();

                            // Step 3: Get the start and end dates
                            DateTime? startDate = Nasdaq100FinancialScraper.Program.globalStartDate;
                            DateTime? endDate = Nasdaq100FinancialScraper.Program.globalEndDate;
                            DateTime? instantDate = Nasdaq100FinancialScraper.Program.globalInstantDate;
                            int quarter = -1;

                            if (!isAnnualReport)
                            {
                                try
                                {
                                    DateTime financialYearStartDate = HTML.HTML.GetFinancialYearStartDate(connection, transaction, companyId);
                                    quarter = HTML.HTML.GetQuarter(startDate.Value, isAnnualReport, financialYearStartDate);
                                }
                                catch (Exception ex)
                                {
                                    //Console.WriteLine($"[ERROR] Failed to retrieve financial year start date for company ID {companyId}: {ex.Message}");
                                    return;
                                }
                            }
                            else
                            {
                                quarter = 0;
                                endDate = instantDate.HasValue ? instantDate.Value : DateTime.Now;
                                startDate = endDate.Value.AddYears(-1);
                            }

                            if (!startDate.HasValue || !endDate.HasValue)
                            {
                                throw new Exception("StartDate or EndDate could not be determined.");
                            }

                            int year = endDate.Value.Year;

                            // Step 4: Get or insert PeriodID
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
                            command.Parameters.AddWithValue("@Year", year);
                            command.Parameters.AddWithValue("@Quarter", quarter);

                            int periodId = Convert.ToInt32(command.ExecuteScalar());

                            // Step 5: Check if period length is valid
                            int dateDifference = (endDate.Value - startDate.Value).Days;
                            if (Math.Abs(dateDifference - 90) > 10 && Math.Abs(dateDifference - 365) > 10)
                            {
                                return;
                            }

                            // Step 6: Save the data
                            command.CommandText = $@"
IF EXISTS (SELECT TOP 1 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
BEGIN
    UPDATE FinancialData
    SET [{elementName}] = @Value, StartDate = @StartDate, EndDate = @EndDate, 
        ParsedFullHTML = CASE WHEN @IsHtmlParsed = 1 THEN 'Yes' ELSE ParsedFullHTML END,
        ParsedFullXBRL = CASE WHEN @IsXbrlParsed = 1 THEN 'Yes' ELSE ParsedFullXBRL END
    WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID;
END
ELSE
BEGIN
    INSERT INTO FinancialData (CompanyID, PeriodID, Year, Quarter, StartDate, EndDate, [{elementName}], ParsedFullHTML, ParsedFullXBRL)
    VALUES (@CompanyID, @PeriodID, @Year, @Quarter, @StartDate, @EndDate, @Value,
            CASE WHEN @IsHtmlParsed = 1 THEN 'Yes' ELSE NULL END,
            CASE WHEN @IsXbrlParsed = 1 THEN 'Yes' ELSE NULL END);
END";

                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@CompanyID", companyId);
                            command.Parameters.AddWithValue("@PeriodID", periodId);
                            command.Parameters.AddWithValue("@Year", year);
                            command.Parameters.AddWithValue("@Quarter", quarter);
                            command.Parameters.AddWithValue("@StartDate", startDate.Value.Date);
                            command.Parameters.AddWithValue("@EndDate", endDate.Value.Date);
                            command.Parameters.AddWithValue("@IsHtmlParsed", isHtmlParsed ? 1 : 0);
                            command.Parameters.AddWithValue("@IsXbrlParsed", isXbrlParsed ? 1 : 0);
                            if (decimal.TryParse(value, out decimal decimalValue))
                            {
                                command.Parameters.AddWithValue("@Value", decimalValue);
                            }
                            else
                            {
                                command.Parameters.AddWithValue("@Value", DBNull.Value);
                            }

                            command.ExecuteNonQuery();

                            // Step 7: Commit the transaction
                            transaction.Commit();
                            //Console.WriteLine($"[INFO] Data saved successfully for company {companyName}, report {year} {quarter}.");
                        }
                        catch (Exception ex)
                        {
                            // Rollback in case of an error
                            transaction.Rollback();
                            //Console.WriteLine($"[ERROR] Failed to save data for {companyName}: {ex.Message}");
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"[ERROR] Failed to process data for batch: {ex.Message}");
            }
        }

        public static async Task ExecuteBatch()
        {
            var batchTimer = Stopwatch.StartNew(); // Timer for batch execution
            const int maxRetries = 3;
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (Nasdaq100FinancialScraper.Program.batchedCommands.Count > 0)
                    {
                        await Nasdaq100FinancialScraper.Program.semaphore.WaitAsync();
                        try
                        {
                            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
                            {
                                await connection.OpenAsync();
                                using (var transaction = connection.BeginTransaction())
                                {
                                    try
                                    {
                                        var commandsToExecute = Nasdaq100FinancialScraper.Program.batchedCommands.ToList();

                                        foreach (var command in commandsToExecute)
                                        {
                                            try
                                            {
                                                using (var clonedCommand = CloneCommand(command))
                                                {
                                                    clonedCommand.Connection = connection;
                                                    clonedCommand.Transaction = transaction;
                                                    //Console.WriteLine($"[INFO] Executing command: {clonedCommand.CommandText}");
                                                    await clonedCommand.ExecuteNonQueryAsync();
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                //Console.WriteLine($"[ERROR] Failed to execute command: {ex.Message}");
                                                throw;
                                            }
                                        }
                                        transaction.Commit();
                                        Nasdaq100FinancialScraper.Program.batchedCommands.Clear();
                                        break;
                                    }
                                    catch (SqlException ex) when (ex.Number == 1205) // SQL Server deadlock error code
                                    {
                                        transaction.Rollback();
                                        //Console.WriteLine($"Transaction deadlock encountered. Attempt {attempt}/{maxRetries}. Retrying...");
                                        if (attempt == maxRetries) throw;
                                    }
                                }
                            }
                        }
                        finally
                        {
                            Nasdaq100FinancialScraper.Program.semaphore.Release();
                        }
                    }
                    else
                    {

                    }
                }
                catch (Exception ex)
                {
                    //Console.WriteLine($"Batch transaction failed. Error: {ex.Message}");
                    if (attempt == maxRetries) throw;
                }
            }
            batchTimer.Stop();
        }
        
                // Track parsed quarters: Dictionary<CompanyID, Dictionary<Year, List<ParsedQuarters>>>
private static Dictionary<int, Dictionary<int, HashSet<int>>> parsedQuarterTracker = new Dictionary<int, Dictionary<int, HashSet<int>>>();
        private static void ResetQuarterTracking(int companyId, int year)
        {
            if (parsedQuarterTracker.ContainsKey(companyId) && parsedQuarterTracker[companyId].ContainsKey(year))
            {
                parsedQuarterTracker[companyId].Remove(year);
            }
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

//namespace Data
//{
//    public class Data
//    {
//        public static void CalculateAndSaveQ4InDatabase(SqlConnection connection, int companyId)
//        {
//            Console.WriteLine($"[INFO] Starting Q4 calculation for CompanyID: {companyId}");

//            // Fetch all years where Quarter = 0 (annual reports)
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
//                Console.WriteLine($"[INFO] Calculating Q4 for Year: {year}");

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
//                            command.Parameters.AddWithValue("@LeewayDays", 5);  // Allow 5 days leeway for matching dates

//                            int rowsAffected = command.ExecuteNonQuery();

//                            Console.WriteLine($"[INFO] Q4 calculation for element: {columnName} complete. Rows affected: {rowsAffected} for CompanyID: {companyId}, Year: {year}");
//                        }
//                    }
//                    catch (Exception ex)
//                    {
//                        Console.WriteLine($"[ERROR] Failed to update Q4 for element: {columnName}, CompanyID: {companyId}, Year: {year}. Error: {ex.Message}");
//                    }
//                }

//                // Update ParsedFullHTML and ParsedFullXBRL fields after the Q4 calculation
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
//                    Console.WriteLine($"[INFO] ParsedFullHTML and ParsedFullXBRL updated for Q4. Rows affected: {parsedRowsAffected} for CompanyID: {companyId}, Year: {year}");
//                }
//            }
//        }



//        public static void CalculateQ4InDatabase(SqlConnection connection, int companyId)
//        {
//            // Log before calculation to verify we are entering the method
//            //Console.WriteLine($"[INFO] Starting Q4 calculation for CompanyID: {companyId}");

//            // Iterate through each element in the FinancialElementLists.ElementsOfInterest
//            foreach (var element in FinancialElementLists.ElementsOfInterest)
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

//                    // Log the constructed SQL query for debugging purposes
//                    //Console.WriteLine($"[DEBUG] Executing Q4 calculation for element: {element}");

//                    using (SqlCommand command = new SqlCommand(q4CalculationQuery, connection))
//                    {
//                        // Add the parameter for CompanyID
//                        command.Parameters.AddWithValue("@CompanyID", companyId);

//                        // Execute the query
//                        int rowsAffected = command.ExecuteNonQuery();

//                        // Log how many rows were affected
//                        //Console.WriteLine($"[INFO] Q4 calculation for element: {element} complete. Rows affected: {rowsAffected} for CompanyID: {companyId}");
//                    }
//                }
//                catch (Exception ex)
//                {
//                    // Log any errors that occur during the execution of each individual column
//                    //Console.WriteLine($"[ERROR] Failed to update Q4 for element: {element}, CompanyID: {companyId}. Error: {ex.Message}");
//                }
//            }
//        }

//        private static void CheckAndFillMissingFinancialYears(int companyId, int year, int quarter)
//        {
//            if (quarter == 0)
//            {
//                // Log that we're handling the annual report
//                Console.WriteLine($"[INFO] Handling annual report for CompanyID: {companyId}, Year: {year} (Quarter = 0)");

//                using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//                {
//                    connection.Open();
//                    using (SqlTransaction transaction = connection.BeginTransaction())
//                    {
//                        try
//                        {
//                            // Step 1: Extract the EndDate year and insert it into the FinancialYear column
//                            string updateFinancialYearQuery = @"
//                    UPDATE FD
//                    SET FD.FinancialYear = YEAR(FD.EndDate)
//                    FROM FinancialData FD
//                    WHERE FD.CompanyID = @CompanyID AND FD.Quarter = 0 AND FD.FinancialYear IS NULL";

//                            using (var command = new SqlCommand(updateFinancialYearQuery, connection, transaction))
//                            {
//                                command.Parameters.AddWithValue("@CompanyID", companyId);

//                                int rowsAffected = command.ExecuteNonQuery();

//                                // Log the result
//                                Console.WriteLine($"[INFO] Updated FinancialYear for {rowsAffected} annual reports for CompanyID: {companyId}.");
//                            }

//                            // Commit transaction
//                            transaction.Commit();
//                        }
//                        catch (Exception ex)
//                        {
//                            Console.WriteLine($"[ERROR] Failed to update FinancialYear for CompanyID: {companyId}. Error: {ex.Message}");
//                            transaction.Rollback();
//                        }
//                    }
//                }
//                return;  // Exit as no further processing is needed for annual reports
//            }

//            // Logic for quarters 1, 2, 3, and 4
//            if (quarter < 1 || quarter > 4)
//            {
//                throw new ArgumentException($"Invalid quarter value: {quarter}");
//            }

//            // Add logic to fill missing financial years for a company for a specific quarter.
//            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//            {
//                connection.Open();
//                using (SqlTransaction transaction = connection.BeginTransaction())
//                {
//                    try
//                    {
//                        // Example: Logic to fill in missing financial year data for the specified quarter
//                        string fillMissingDataQuery = @"
//                UPDATE FinancialData
//                SET FinancialYear = @Year
//                WHERE CompanyID = @CompanyID AND Quarter = @Quarter AND Year IS NULL";

//                        using (var command = new SqlCommand(fillMissingDataQuery, connection, transaction))
//                        {
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@Year", year);
//                            command.Parameters.AddWithValue("@Quarter", quarter);

//                            int rowsAffected = command.ExecuteNonQuery();
//                            Console.WriteLine($"[INFO] Filled missing financial year for {rowsAffected} records. CompanyID: {companyId}, Year: {year}, Quarter: {quarter}");
//                        }

//                        // Commit the transaction
//                        transaction.Commit();
//                    }
//                    catch (Exception ex)
//                    {
//                        Console.WriteLine($"[ERROR] Failed to fill missing financial year data. Error: {ex.Message}");
//                        transaction.Rollback();
//                    }
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
//        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, bool isHtmlParsed, bool isXbrlParsed, int leewayDays = 5)
//        {
//            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//            {
//                connection.Open();
//                SqlTransaction transaction = connection.BeginTransaction();

//                try
//                {
//                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
//                    {
//                        // Step 1: Retrieve Company details (Name and Symbol)
//                        command.CommandText = @"SELECT CompanyName, CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
//                        command.Parameters.AddWithValue("@CompanyID", companyId);

//                        using (var reader = command.ExecuteReader())
//                        {
//                            if (!reader.Read())
//                            {
//                                Console.WriteLine($"[ERROR] No matching company found for CompanyID: {companyId}");
//                                return;
//                            }
//                            reader.Close();
//                        }

//                        int financialYearStartMonth = GetCompanyFiscalStartMonth(companyId, connection, transaction);

//                        // Step 2: Calculate Quarter
//                        if (quarter != 0)
//                        {
//                            quarter = CalculateQuarterBasedOnFiscalYear(endDate, financialYearStartMonth);
//                        }

//                        // Step 3: Calculate Financial Year
//                        int financialYear = (quarter == 0) ? endDate.Year : Nasdaq100FinancialScraper.Program.CalculateFinancialYear(companyId, endDate, connection, transaction);

//                        // Step 4: Fetch or create the PeriodID
//                        command.CommandText = @"
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
//                        command.Parameters.Clear();
//                        command.Parameters.AddWithValue("@Year", financialYear);
//                        command.Parameters.AddWithValue("@Quarter", quarter);

//                        int periodId = Convert.ToInt32(command.ExecuteScalar());

//                        // Step 5: Calculate the start date for annual or quarterly reports
//                        DateTime startDate = (quarter == 0) ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);

//                        // Step 6: Check for existing rows with the same CompanyID, StartDate, and EndDate (with leeway)
//                        command.CommandText = @"
//SELECT COUNT(*) 
//FROM FinancialData 
//WHERE CompanyID = @CompanyID 
//AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
//AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
//                        command.Parameters.Clear();
//                        command.Parameters.AddWithValue("@CompanyID", companyId);
//                        command.Parameters.AddWithValue("@StartDate", startDate);
//                        command.Parameters.AddWithValue("@EndDate", endDate);
//                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);

//                        int existingRowCount = (int)command.ExecuteScalar();
//                        bool hasExistingRow = existingRowCount > 0;

//                        // Step 7: Update or insert the row in the FinancialData table (excluding HTML/XBRL)
//                        if (hasExistingRow)
//                        {
//                            command.CommandText = @"
//    UPDATE FinancialData 
//    SET 
//        FinancialYear = COALESCE(@FinancialYear, FinancialYear), 
//        StartDate = COALESCE(@StartDate, StartDate), 
//        EndDate = COALESCE(@EndDate, EndDate)
//    WHERE CompanyID = @CompanyID 
//    AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
//    AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
//                        }
//                        else
//                        {
//                            command.CommandText = @"
//    INSERT INTO FinancialData (CompanyID, PeriodID, FinancialYear, Year, Quarter, StartDate, EndDate)
//    VALUES (@CompanyID, @PeriodID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate)";
//                        }

//                        command.Parameters.Clear();
//                        command.Parameters.AddWithValue("@CompanyID", companyId);
//                        command.Parameters.AddWithValue("@PeriodID", periodId);
//                        command.Parameters.AddWithValue("@FinancialYear", financialYear);
//                        command.Parameters.AddWithValue("@Year", endDate.Year);
//                        command.Parameters.AddWithValue("@Quarter", quarter);
//                        command.Parameters.AddWithValue("@StartDate", startDate);
//                        command.Parameters.AddWithValue("@EndDate", endDate);
//                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);

//                        Nasdaq100FinancialScraper.Program.batchedCommands.Add(CloneCommand(command));

//                        // Step 8: Handle additional element-specific updates (if applicable)
//                        if (elementName != "AnnualReport" && elementName != "QuarterlyReport")
//                        {
//                            string columnName = Nasdaq100FinancialScraper.Program.GetColumnName(elementName);
//                            if (!string.IsNullOrEmpty(columnName))
//                            {
//                                command.CommandText = $@"
//        UPDATE FinancialData 
//        SET [{columnName}] = @Value
//        WHERE CompanyID = @CompanyID 
//        AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
//        AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
//                                command.Parameters.Clear();
//                                command.Parameters.AddWithValue("@CompanyID", companyId);
//                                command.Parameters.AddWithValue("@StartDate", startDate);
//                                command.Parameters.AddWithValue("@EndDate", endDate);
//                                command.Parameters.AddWithValue("@Value", value);
//                                command.Parameters.AddWithValue("@LeewayDays", leewayDays);

//                                Nasdaq100FinancialScraper.Program.batchedCommands.Add(CloneCommand(command));
//                            }
//                        }

//                        // Step 9: Update ParsedFullHTML and ParsedFullXBRL status only if parsing is completed
//                        if (isHtmlParsed || isXbrlParsed)
//                        {
//                            command.CommandText = @"
//    UPDATE FinancialData 
//    SET ParsedFullHTML = CASE WHEN @ParsedFullHTML = 'Yes' THEN 'Yes' ELSE ParsedFullHTML END,
//        ParsedFullXBRL = CASE WHEN @ParsedFullXBRL = 'Yes' THEN 'Yes' ELSE ParsedFullXBRL END
//    WHERE CompanyID = @CompanyID 
//    AND StartDate BETWEEN DATEADD(DAY, -@LeewayDays, @StartDate) AND DATEADD(DAY, @LeewayDays, @StartDate)
//    AND EndDate BETWEEN DATEADD(DAY, -@LeewayDays, @EndDate) AND DATEADD(DAY, @LeewayDays, @EndDate)";
//                            command.Parameters.Clear();
//                            command.Parameters.AddWithValue("@CompanyID", companyId);
//                            command.Parameters.AddWithValue("@StartDate", startDate);
//                            command.Parameters.AddWithValue("@EndDate", endDate);
//                            command.Parameters.AddWithValue("@ParsedFullHTML", isHtmlParsed ? "Yes" : DBNull.Value);
//                            command.Parameters.AddWithValue("@ParsedFullXBRL", isXbrlParsed ? "Yes" : DBNull.Value);
//                            command.Parameters.AddWithValue("@LeewayDays", leewayDays);

//                            Nasdaq100FinancialScraper.Program.batchedCommands.Add(CloneCommand(command));
//                        }
//                    }

//                    // Step 10: Mark the quarter as parsed and check for Q4 calculation
//                    MarkQuarterAsParsed(companyId, endDate.Year, quarter);  // Track the parsed quarter
//                    Console.WriteLine($"[DEBUG] Quarter {quarter} parsed for CompanyID: {companyId}, Year: {endDate.Year}");

//                    // Check if all three quarters (Q1, Q2, and Q3) have been parsed
//                    if (AreAllQuartersParsed(companyId, endDate.Year))
//                    {
//                        Console.WriteLine($"[INFO] All quarters parsed for CompanyID: {companyId}, Year: {endDate.Year}. Triggering Q4 calculation.");
//                        //CalculateMissingQuarter(companyId, endDate.Year).Wait();
//                    }

//                    transaction.Commit();  // Commit the transaction
//                }
//                catch (Exception ex)
//                {
//                    transaction.Rollback();
//                    Console.WriteLine($"[ERROR] Transaction rolled back due to error: {ex.Message}");
//                    throw;
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

//            // Mark the quarter as parsed
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
//        private static int GetCompanyFiscalStartMonth(int companyId, SqlConnection connection, SqlTransaction transaction)
//        {
//            using (var command = new SqlCommand())
//            {
//                command.Connection = connection;
//                command.Transaction = transaction;

//                // Try to get the fiscal start month from the FinancialData table by finding the earliest entry
//                command.CommandText = @"
//            SELECT TOP 1 MONTH(StartDate) 
//            FROM FinancialData 
//            WHERE CompanyID = @CompanyID 
//            ORDER BY StartDate ASC";
//                command.Parameters.AddWithValue("@CompanyID", companyId);

//                object result = command.ExecuteScalar();
//                if (result != null)
//                {
//                    // Return the month as the fiscal start month
//                    return (int)result;
//                }
//                else
//                {
//                    // Default to January if no data is found
//                    return 1;
//                }
//            }
//        }

//        public static int GetQuarterFromEndDate(DateTime endDate, int companyId, SqlConnection connection, SqlTransaction transaction)
//        {
//            // Fetch fiscal start month using the earliest start date, now with the transaction parameter
//            int fiscalStartMonth = GetCompanyFiscalStartMonth(companyId, connection, transaction);

//            // Map end date to the correct quarter based on the fiscal year start month
//            if (fiscalStartMonth == 1) // Fiscal year starts in January
//            {
//                if (endDate.Month <= 3) return 1;
//                else if (endDate.Month <= 6) return 2;
//                else if (endDate.Month <= 9) return 3;
//                else return 4;
//            }
//            else
//            {
//                // Adjust quarter calculation based on non-standard fiscal year starts
//                int fiscalMonthOffset = (endDate.Month - fiscalStartMonth + 12) % 12;
//                if (fiscalMonthOffset < 3) return 1;
//                else if (fiscalMonthOffset < 6) return 2;
//                else if (fiscalMonthOffset < 9) return 3;
//                else return 4;
//            }
//        }
//        public static int CalculateQuarterBasedOnFiscalYear(DateTime endDate, int fiscalStartMonth, int leewayDays = 14)
//        {
//            // Subtract exactly 364 days from the end date to determine the fiscal year start
//            DateTime fiscalYearStart = endDate.AddDays(-364);

//            // Apply leeway for fiscal year shifts based on the reporting cycle
//            DateTime fiscalYearStartWithLeeway = fiscalYearStart.AddDays(-leewayDays);
//            if (endDate < fiscalYearStartWithLeeway)
//            {
//                fiscalYearStart = fiscalYearStart.AddYears(-1);
//            }
//            int monthInFiscalYear = ((endDate.Month - fiscalYearStart.Month + 12) % 12) + 1;
//            if (monthInFiscalYear <= 3) return 1;
//            if (monthInFiscalYear <= 6) return 2;
//            if (monthInFiscalYear <= 9) return 3;
//            return 4;
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
//            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//            {
//                await connection.OpenAsync();
//                string query = @"
//                    SELECT CompanyID, PeriodID, Year, Quarter, ParsedFullHTML, ParsedFullXBRL 
//                    FROM FinancialData 
//                    WHERE ParsedFullHTML IS NULL OR ParsedFullXBRL IS NULL 
//                    OR ParsedFullHTML != 'Yes' OR ParsedFullXBRL != 'Yes'";
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
//                                await HTML.HTML.ReparseHtmlReports(companyId, periodId, year, quarter, companyName, companySymbol);
//                            }
//                            if (needsXbrlParsing)
//                            {
//                                await XBRL.XBRL.ReparseXbrlReports(companyId, periodId, year, quarter, companyName, companySymbol);
//                            }
//                        }));
//                    }

//                    await Task.WhenAll(tasks);
//                }
//            }
//            await ExecuteBatch();
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

//        public static void SaveToDatabase(string elementName, string value, XElement? context, string[] elementsOfInterest, List<FinancialElement> elements, bool isAnnualReport, string companyName, string companySymbol, bool isHtmlParsed = false, bool isXbrlParsed = false)
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

//                            // Step 2: Get the CompanyID
//                            command.CommandText = "SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
//                            int companyId = (int)command.ExecuteScalar();

//                            // Step 3: Get the start and end dates
//                            DateTime? startDate = Nasdaq100FinancialScraper.Program.globalStartDate;
//                            DateTime? endDate = Nasdaq100FinancialScraper.Program.globalEndDate;
//                            DateTime? instantDate = Nasdaq100FinancialScraper.Program.globalInstantDate;
//                            int quarter = -1;

//                            if (!isAnnualReport)
//                            {
//                                try
//                                {
//                                    DateTime financialYearStartDate = HTML.HTML.GetFinancialYearStartDate(connection, transaction, companyId);
//                                    quarter = HTML.HTML.GetQuarter(startDate.Value, isAnnualReport, financialYearStartDate);
//                                }
//                                catch (Exception ex)
//                                {
//                                    //Console.WriteLine($"[ERROR] Failed to retrieve financial year start date for company ID {companyId}: {ex.Message}");
//                                    return;
//                                }
//                            }
//                            else
//                            {
//                                quarter = 0;
//                                endDate = instantDate.HasValue ? instantDate.Value : DateTime.Now;
//                                startDate = endDate.Value.AddYears(-1);
//                            }

//                            if (!startDate.HasValue || !endDate.HasValue)
//                            {
//                                throw new Exception("StartDate or EndDate could not be determined.");
//                            }

//                            int year = endDate.Value.Year;

//                            // Step 4: Get or insert PeriodID
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

//                            int periodId = Convert.ToInt32(command.ExecuteScalar());

//                            // Step 5: Check if period length is valid
//                            int dateDifference = (endDate.Value - startDate.Value).Days;
//                            if (Math.Abs(dateDifference - 90) > 10 && Math.Abs(dateDifference - 365) > 10)
//                            {
//                                return;
//                            }

//                            // Step 6: Save the data
//                            command.CommandText = $@"
//IF EXISTS (SELECT TOP 1 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
//BEGIN
//    UPDATE FinancialData
//    SET [{elementName}] = @Value, StartDate = @StartDate, EndDate = @EndDate, 
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
//                            if (decimal.TryParse(value, out decimal decimalValue))
//                            {
//                                command.Parameters.AddWithValue("@Value", decimalValue);
//                            }
//                            else
//                            {
//                                command.Parameters.AddWithValue("@Value", DBNull.Value);
//                            }

//                            command.ExecuteNonQuery();

//                            // Step 7: Commit the transaction
//                            transaction.Commit();
//                            //Console.WriteLine($"[INFO] Data saved successfully for company {companyName}, report {year} {quarter}.");
//                        }
//                        catch (Exception ex)
//                        {
//                            // Rollback in case of an error
//                            transaction.Rollback();
//                            //Console.WriteLine($"[ERROR] Failed to save data for {companyName}: {ex.Message}");
//                            throw;
//                        }
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                //Console.WriteLine($"[ERROR] Failed to process data for batch: {ex.Message}");
//            }
//        }

//        public static async Task ExecuteBatch()
//        {
//            var batchTimer = Stopwatch.StartNew(); // Timer for batch execution
//            const int maxRetries = 3;
//            for (int attempt = 1; attempt <= maxRetries; attempt++)
//            {
//                try
//                {
//                    if (Nasdaq100FinancialScraper.Program.batchedCommands.Count > 0)
//                    {
//                        await Nasdaq100FinancialScraper.Program.semaphore.WaitAsync();
//                        try
//                        {
//                            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
//                            {
//                                await connection.OpenAsync();
//                                using (var transaction = connection.BeginTransaction())
//                                {
//                                    try
//                                    {
//                                        var commandsToExecute = Nasdaq100FinancialScraper.Program.batchedCommands.ToList();

//                                        foreach (var command in commandsToExecute)
//                                        {
//                                            try
//                                            {
//                                                using (var clonedCommand = CloneCommand(command))
//                                                {
//                                                    clonedCommand.Connection = connection;
//                                                    clonedCommand.Transaction = transaction;
//                                                    //Console.WriteLine($"[INFO] Executing command: {clonedCommand.CommandText}");
//                                                    await clonedCommand.ExecuteNonQueryAsync();
//                                                }
//                                            }
//                                            catch (Exception ex)
//                                            {
//                                                //Console.WriteLine($"[ERROR] Failed to execute command: {ex.Message}");
//                                                throw;
//                                            }
//                                        }
//                                        transaction.Commit();
//                                        Nasdaq100FinancialScraper.Program.batchedCommands.Clear();
//                                        break;
//                                    }
//                                    catch (SqlException ex) when (ex.Number == 1205) // SQL Server deadlock error code
//                                    {
//                                        transaction.Rollback();
//                                        //Console.WriteLine($"Transaction deadlock encountered. Attempt {attempt}/{maxRetries}. Retrying...");
//                                        if (attempt == maxRetries) throw;
//                                    }
//                                }
//                            }
//                        }
//                        finally
//                        {
//                            Nasdaq100FinancialScraper.Program.semaphore.Release();
//                        }
//                    }
//                    else
//                    {

//                    }
//                }
//                catch (Exception ex)
//                {
//                    //Console.WriteLine($"Batch transaction failed. Error: {ex.Message}");
//                    if (attempt == maxRetries) throw;
//                }
//            }
//            batchTimer.Stop();
//        }

//        // Track parsed quarters: Dictionary<CompanyID, Dictionary<Year, List<ParsedQuarters>>>
//        private static Dictionary<int, Dictionary<int, HashSet<int>>> parsedQuarterTracker = new Dictionary<int, Dictionary<int, HashSet<int>>>();
//        private static void ResetQuarterTracking(int companyId, int year)
//        {
//            if (parsedQuarterTracker.ContainsKey(companyId) && parsedQuarterTracker[companyId].ContainsKey(year))
//            {
//                parsedQuarterTracker[companyId].Remove(year);
//            }
//        }

//    }
//}