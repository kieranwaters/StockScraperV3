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
using System.Collections.Concurrent;
using System.Data;

namespace Data
{
    public static class Data
    {
        private static void UpdateFinancialYear(int companyId, int quarter, DateTime startDate, DateTime endDate, int financialYear, SqlConnection connection, SqlTransaction transaction)
        {
            using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
            {
                command.CommandText = @"
            UPDATE FinancialData
            SET FinancialYear = @FinancialYear
            WHERE CompanyID = @CompanyID
            AND Quarter = @Quarter
            AND CAST(StartDate AS DATE) = @StartDate
            AND CAST(EndDate AS DATE) = @EndDate";
                command.Parameters.AddWithValue("@FinancialYear", financialYear);
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.Parameters.AddWithValue("@Quarter", quarter);
                command.Parameters.AddWithValue("@StartDate", startDate.Date);
                command.Parameters.AddWithValue("@EndDate", endDate.Date);
                int rowsAffected = command.ExecuteNonQuery();
            }
        }
        public static void UpdateFinancialYearForCompany(int companyId)
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                connection.Open();
                SqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
                    {
                        command.CommandText = @"
                    SELECT FinancialDataID, EndDate, Quarter, StartDate
                    FROM FinancialData
                    WHERE CompanyID = @CompanyID AND FinancialYear IS NULL";
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        var rowsToUpdate = new List<(int financialDataId, DateTime endDate, int quarter, DateTime startDate)>();
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                int financialDataId = reader.GetInt32(0);
                                DateTime endDate = reader.GetDateTime(1);
                                int quarter = reader.GetInt32(2);
                                DateTime startDate = reader.GetDateTime(3);
                                rowsToUpdate.Add((financialDataId, endDate, quarter, startDate));
                            }
                        }
                        foreach (var row in rowsToUpdate)
                        {
                            int financialYear = CalculateFinancialYearBasedOnQuarter(row.endDate, row.quarter);
                            UpdateFinancialYear(companyId, row.quarter, row.startDate, row.endDate, financialYear, connection, transaction);
                        }
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Failed to update financial year for CompanyID: {companyId}. Error: {ex.Message}");
                    transaction.Rollback();
                }
            }
        }
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
        public static void FinalizeCompanyData(int companyId)
        {
            UpdateFinancialYearForCompany(companyId);
        }

        public static DateTime GetFiscalYearEndForSpecificYear(int companyId, int year, SqlConnection connection, SqlTransaction transaction)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandText = @"
            SELECT TOP 1 EndDate
            FROM FinancialData WITH (INDEX(IX_CompanyID_Year_Quarter))  -- Use index if available
            WHERE CompanyID = @CompanyID AND Year <= @Year AND Quarter = 0
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
        public static Dictionary<int, DateTime?> companyFiscalYearStartDates = new Dictionary<int, DateTime?>();
        public static int CalculateQuarterByFiscalDayMonth(int companyId, DateTime reportDate, SqlConnection connection, SqlTransaction transaction, int leewayDays = 15)
        {
            DateTime fiscalYearEnd = GetFiscalYearEndForSpecificYear(companyId, reportDate.Year, connection, transaction);
            DateTime fiscalYearStart = fiscalYearEnd.AddYears(-1).AddDays(1);
            if (reportDate > fiscalYearEnd)
            {
                fiscalYearStart = fiscalYearEnd.AddDays(1);
                fiscalYearEnd = fiscalYearEnd.AddYears(1);
            }
            int daysFromFiscalStart = (reportDate - fiscalYearStart).Days;
            int q1Boundary = 90 + leewayDays;
            int q2Boundary = 180 + leewayDays;
            int q3Boundary = 270 + leewayDays;
            if (daysFromFiscalStart < q1Boundary) return 1;
            if (daysFromFiscalStart < q2Boundary) return 2;
            if (daysFromFiscalStart < q3Boundary) return 3;
            return 4;
        }
        public static void SaveQuarterData(int companyId, DateTime endDate, int quarter, string elementName, decimal value, bool isHtmlParsed, bool isXbrlParsed, List<SqlCommand> batchedCommands, int leewayDays = 15)
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                connection.Open();
                SqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    using (var command = new SqlCommand { Connection = connection, Transaction = transaction })
                    {//Retrieve Company details (Name and Symbol)
                        command.CommandText = @"SELECT CompanyName, CompanySymbol FROM CompaniesList WHERE CompanyID = @CompanyID";
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        using (var reader = command.ExecuteReader())
                        {
                            if (!reader.Read())// No matching company found
                            {
                                return;
                            }
                            reader.Close();
                        }
                        // Ensure quarter is calculated correctly using the updated method
                        quarter = CalculateQuarterByFiscalDayMonth(companyId, endDate, connection, transaction, leewayDays);
                        Console.WriteLine($"[DEBUG] Calculated quarter: {quarter} for CompanyID: {companyId}, EndDate: {endDate}");
                        int financialYear = (quarter == 0) ? endDate.Year : Nasdaq100FinancialScraper.Program.CalculateFinancialYear(companyId, endDate, connection, transaction);
                        Console.WriteLine($"[DEBUG] Calculated financial year: {financialYear} for CompanyID: {companyId}, Quarter: {quarter}");
                        DateTime startDate = (quarter == 0) ? endDate.AddYears(-1).AddDays(1) : endDate.AddMonths(-3);
                        int dateDifference = (endDate - startDate).Days;
                        //Check for existing rows using CompanyID, StartDate, and EndDate (with leeway)
                        command.CommandText = @"
                SELECT COUNT(*) 
                FROM FinancialData 
                WHERE CompanyID = @CompanyID 
                AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
                AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        command.Parameters.AddWithValue("@StartDate", startDate);
                        command.Parameters.AddWithValue("@EndDate", endDate);
                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);
                        int existingRowCount = (int)command.ExecuteScalar();
                        bool hasExistingRow = existingRowCount > 0;
                        if (hasExistingRow)//Use FinancialYear for updating or inserting rows in FinancialData
                        {
                            command.CommandText = @"
                    UPDATE FinancialData 
                    SET 
                        FinancialYear = CASE WHEN @FinancialYear IS NOT NULL THEN @FinancialYear ELSE FinancialYear END, 
                        StartDate = CASE WHEN @StartDate IS NOT NULL THEN @StartDate ELSE StartDate END, 
                        EndDate = CASE WHEN @EndDate IS NOT NULL THEN @EndDate ELSE EndDate END
                    WHERE CompanyID = @CompanyID 
                    AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
                    AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
                        }
                        else
                        {
                            command.CommandText = @"
                    INSERT INTO FinancialData (CompanyID, FinancialYear, Year, Quarter, StartDate, EndDate)
                    VALUES (@CompanyID, @FinancialYear, @Year, @Quarter, @StartDate, @EndDate)";
                        }
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        command.Parameters.AddWithValue("@FinancialYear", financialYear);
                        command.Parameters.AddWithValue("@Year", endDate.Year);
                        command.Parameters.AddWithValue("@Quarter", quarter);
                        command.Parameters.AddWithValue("@StartDate", startDate);
                        command.Parameters.AddWithValue("@EndDate", endDate);
                        command.Parameters.AddWithValue("@LeewayDays", leewayDays);
                        batchedCommands.Add(CloneCommand(command));
                        if (elementName != "AnnualReport" && elementName != "QuarterlyReport")
                        {
                            string columnName = Nasdaq100FinancialScraper.Program.GetColumnName(elementName);
                            if (!string.IsNullOrEmpty(columnName))
                            {
                                command.CommandText = $@"
                        UPDATE FinancialData 
                        SET [{columnName}] = @Value
                        WHERE CompanyID = @CompanyID 
                        AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
                        AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@CompanyID", companyId);
                                command.Parameters.AddWithValue("@StartDate", startDate);
                                command.Parameters.AddWithValue("@EndDate", endDate);
                                command.Parameters.AddWithValue("@Value", value);
                                command.Parameters.AddWithValue("@LeewayDays", leewayDays);
                                batchedCommands.Add(CloneCommand(command));
                            }
                        }
                        if (isHtmlParsed || isXbrlParsed)
                        {
                            command.CommandText = @"
                    UPDATE FinancialData 
                    SET ParsedFullHTML = CASE WHEN @ParsedFullHTML = 'Yes' THEN 'Yes' ELSE ParsedFullHTML END,
                        ParsedFullXBRL = CASE WHEN @ParsedFullXBRL = 'Yes' THEN 'Yes' ELSE ParsedFullXBRL END
                    WHERE CompanyID = @CompanyID 
                    AND ABS(DATEDIFF(DAY, StartDate, @StartDate)) <= @LeewayDays
                    AND ABS(DATEDIFF(DAY, EndDate, @EndDate)) <= @LeewayDays";
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@CompanyID", companyId);
                            command.Parameters.AddWithValue("@StartDate", startDate);
                            command.Parameters.AddWithValue("@EndDate", endDate);
                            command.Parameters.AddWithValue("@ParsedFullHTML", isHtmlParsed ? "Yes" : DBNull.Value);
                            command.Parameters.AddWithValue("@ParsedFullXBRL", isXbrlParsed ? "Yes" : DBNull.Value);
                            command.Parameters.AddWithValue("@LeewayDays", leewayDays);
                            batchedCommands.Add(CloneCommand(command));
                        }
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                }
                FinalizeCompanyData(companyId);
            }
        }
        public static void SaveToDatabase(string elementName, string value, XElement? context, string[] elementsOfInterest, List<FinancialElement> elements, bool isAnnualReport, string companyName, string companySymbol, DateTime? startDate, DateTime? endDate, bool isHtmlParsed = false, bool isXbrlParsed = false, int leewayDays = 15)
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
                        {   // Insert company if not exists
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
                            command.CommandText = "SELECT TOP 1 CompanyID FROM CompaniesList WHERE CompanySymbol = @CompanySymbol";
                            int companyId = (int)command.ExecuteScalar();
                            if (!startDate.HasValue || !endDate.HasValue)
                            {
                                throw new Exception("StartDate or EndDate is not provided.");
                            }
                            int fiscalMonth = 1;
                            int fiscalDay = 1;
                            if (!companyFiscalYearStartDates.TryGetValue(companyId, out DateTime? fiscalYearStartDate) || !fiscalYearStartDate.HasValue)
                            {
                                command.CommandText = @"
SELECT TOP 1 MONTH(StartDate) AS FiscalMonth, DAY(StartDate) AS FiscalDay
FROM FinancialData 
WHERE CompanyID = @CompanyID 
AND Quarter = 0  -- Annual reports only
ORDER BY EndDate DESC";
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@CompanyID", companyId);
                                using (var fiscalReader = command.ExecuteReader())
                                {
                                    if (fiscalReader.Read())
                                    {
                                        fiscalMonth = fiscalReader.GetInt32(0);
                                        fiscalDay = fiscalReader.GetInt32(1);
                                        fiscalYearStartDate = new DateTime(DateTime.Now.Year, fiscalMonth, fiscalDay);
                                        companyFiscalYearStartDates[companyId] = fiscalYearStartDate; // Store in dictionary
                                    }
                                    fiscalReader.Close();
                                }
                                if (!fiscalYearStartDate.HasValue)
                                {
                                    fiscalYearStartDate = new DateTime(DateTime.Now.Year, 1, 1);
                                    companyFiscalYearStartDates[companyId] = fiscalYearStartDate;
                                }
                            }
                            else
                            {
                                fiscalMonth = fiscalYearStartDate.Value.Month;
                                fiscalDay = fiscalYearStartDate.Value.Day;
                            }
                            int quarter;
                            if (isAnnualReport)
                            {
                                quarter = 0;
                            }
                            else
                            {
                                quarter = CalculateQuarterByFiscalDayMonth(companyId, endDate.Value, connection, transaction, leewayDays);
                            }
                            int year = endDate.Value.Year;
                            if (!isAnnualReport)
                            {
                                startDate = endDate.Value.AddMonths(-3).AddDays(1);
                            }
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
                            object periodIdObject = command.ExecuteScalar();
                            // If PeriodID could not be generated, rollback and stop the process
                            if (periodIdObject == null || periodIdObject == DBNull.Value || Convert.ToInt32(periodIdObject) <= 0)
                            {
                                transaction.Rollback();
                                return; // Stop processing if PeriodID is invalid
                            }
                            int periodId = Convert.ToInt32(periodIdObject);
                            // Ensure at least one financial field or parsed flag is being updated
                            bool hasValidFinancialData = !string.IsNullOrEmpty(value) && decimal.TryParse(value, out _);
                            bool hasOnlyParsedData = !hasValidFinancialData && (isHtmlParsed || isXbrlParsed);
                            if (!hasValidFinancialData && !hasOnlyParsedData)
                            {
                                transaction.Rollback();
                                return; // No valid data to save, don't insert row
                            }
                            // Save or update the data
                            command.CommandText = $@"
IF EXISTS (SELECT 1 FROM FinancialData WHERE CompanyID = @CompanyID AND PeriodID = @PeriodID)
BEGIN
    UPDATE FinancialData
    SET [{elementName}] = @Value,
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
                            command.Parameters.AddWithValue("@Value", hasValidFinancialData ? Convert.ToDecimal(value) : (object)DBNull.Value);
                            command.ExecuteNonQuery();
                            RemoveEmptyFinancialDataRows(companyId, connection, transaction);
                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to process data for {companyName}: {ex.Message}", ex);
            }
        }
        public static void RemoveEmptyFinancialDataRows(int companyId, SqlConnection connection, SqlTransaction transaction)
        {   // Combine financial columns from all three lists: ElementsOfInterest, InstantDateElements, and HTMLElementsOfInterest
            var financialColumns = new HashSet<string>(FinancialElementLists.ElementsOfInterest);
            financialColumns.UnionWith(FinancialElementLists.InstantDateElements);
            financialColumns.UnionWith(FinancialElementLists.HTMLElementsOfInterest.Values.Select(v => v.ColumnName));
            // Create a dynamic SQL query that checks if all relevant financial columns are NULL
            string financialColumnsCheck = string.Join(" IS NULL AND ", financialColumns) + " IS NULL";
            string sqlCommand = $@"
        DELETE FROM FinancialData
        WHERE CompanyID = @CompanyID
        AND {financialColumnsCheck}
        ";
            using (SqlCommand command = new SqlCommand(sqlCommand, connection, transaction))
            {
                command.Parameters.AddWithValue("@CompanyID", companyId);
                command.ExecuteNonQuery();
            }
        }
        public static void MergeDuplicateFinancialDataRecords(int leewayDays = 15)
        {
            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
            {
                connection.Open();
                SqlTransaction transaction = connection.BeginTransaction();
                try
                {  // Step 1: Identify duplicate groups
                    string duplicateGroupsQuery = @"
            SELECT CompanyID, Quarter, 
                   MIN(StartDate) AS StartDate, MAX(EndDate) AS EndDate,
                   COUNT(*) AS DuplicateCount
            FROM FinancialData
            GROUP BY CompanyID, Quarter, YEAR(StartDate), YEAR(EndDate)
            HAVING COUNT(*) > 1";
                    DataTable duplicateGroups = new DataTable();
                    using (SqlCommand command = new SqlCommand(duplicateGroupsQuery, connection, transaction))
                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(duplicateGroups);
                    }
                    foreach (DataRow row in duplicateGroups.Rows)
                    {
                        int companyId = (int)row["CompanyID"];
                        int quarter = (int)row["Quarter"];
                        DateTime startDate = (DateTime)row["StartDate"];
                        DateTime endDate = (DateTime)row["EndDate"];
                        string duplicatesQuery = @"
                SELECT FinancialDataID, StartDate, EndDate, *
                FROM FinancialData
                WHERE CompanyID = @CompanyID AND Quarter = @Quarter
                AND DATEDIFF(DAY, @StartDate, StartDate) BETWEEN -@LeewayDays AND @LeewayDays
                AND DATEDIFF(DAY, @EndDate, EndDate) BETWEEN -@LeewayDays AND @LeewayDays";
                        List<DataRow> duplicateRecords = new List<DataRow>();
                        using (SqlCommand command = new SqlCommand(duplicatesQuery, connection, transaction))
                        {
                            command.Parameters.AddWithValue("@CompanyID", companyId);
                            command.Parameters.AddWithValue("@Quarter", quarter);
                            command.Parameters.AddWithValue("@StartDate", startDate);
                            command.Parameters.AddWithValue("@EndDate", endDate);
                            command.Parameters.AddWithValue("@LeewayDays", leewayDays);
                            using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                            {
                                DataTable duplicatesTable = new DataTable();
                                adapter.Fill(duplicatesTable);
                                duplicateRecords.AddRange(duplicatesTable.AsEnumerable());
                            }
                        }
                        if (duplicateRecords.Count > 1)
                        {
                            DataRow mergedRecord = MergeFinancialDataRecords(duplicateRecords);
                            string deleteQuery = @"
                    DELETE FROM FinancialData
                    WHERE FinancialDataID IN ({0})";
                            string idsToDelete = string.Join(",", duplicateRecords.Select(r => r["FinancialDataID"].ToString()));
                            using (SqlCommand deleteCommand = new SqlCommand(string.Format(deleteQuery, idsToDelete), connection, transaction))
                            {
                                deleteCommand.ExecuteNonQuery();
                            }
                            InsertMergedFinancialDataRecord(mergedRecord, connection, transaction);
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine($"Error merging duplicates: {ex.Message}");
                    throw;
                }
            }
        }
        private static DataRow MergeFinancialDataRecords(List<DataRow> records)
        {  // Assume all records have the same CompanyID, Quarter, StartDate, EndDate
            DataRow baseRecord = records[0];
            foreach (DataRow record in records.Skip(1))
            {
                foreach (DataColumn column in baseRecord.Table.Columns)
                {
                    if (column.ColumnName == "CompanyID" ||
                        column.ColumnName == "Year" ||
                        column.ColumnName == "Quarter" ||
                        column.ColumnName == "StartDate" ||
                        column.ColumnName == "EndDate")
                    {
                        continue; // Skip key columns
                    }
                    object baseValue = baseRecord[column];
                    object newValue = record[column];
                    if (baseValue == DBNull.Value && newValue != DBNull.Value)
                    {
                        baseRecord[column] = newValue;
                    }
                    else if (baseValue != DBNull.Value && newValue != DBNull.Value)
                    {
                        // Handle conflicts if needed
                        // For now, we'll keep the existing value
                    }
                }
            }
            return baseRecord;
        }
        private static void InsertMergedFinancialDataRecord(DataRow record, SqlConnection connection, SqlTransaction transaction)
        {            // Build the INSERT statement dynamically based on columns
            StringBuilder columnsBuilder = new StringBuilder();
            StringBuilder valuesBuilder = new StringBuilder();
            foreach (DataColumn column in record.Table.Columns)
            {
                if (column.ColumnName == "FinancialDataID")
                {
                    continue; // Skip the ID column
                }
                columnsBuilder.Append($"[{column.ColumnName}],");
                valuesBuilder.Append($"@{column.ColumnName},");
            }
            string columns = columnsBuilder.ToString().TrimEnd(',');
            string values = valuesBuilder.ToString().TrimEnd(',');
            string insertQuery = $"INSERT INTO FinancialData ({columns}) VALUES ({values})";
            using (SqlCommand command = new SqlCommand(insertQuery, connection, transaction))
            {
                foreach (DataColumn column in record.Table.Columns)
                {
                    if (column.ColumnName == "FinancialDataID")
                    {
                        continue; // Skip the ID column
                    }
                    object value = record[column];
                    command.Parameters.AddWithValue($"@{column.ColumnName}", value ?? DBNull.Value);
                }
                command.ExecuteNonQuery();
            }
        }
        private static Dictionary<int, Dictionary<int, HashSet<int>>> parsedQuarterTracker = new Dictionary<int, Dictionary<int, HashSet<int>>>();
        private static void ResetQuarterTracking(int companyId, int year)
        {
            if (parsedQuarterTracker.ContainsKey(companyId))
            {
                if (parsedQuarterTracker[companyId].ContainsKey(year))
                {
                    parsedQuarterTracker[companyId][year].Clear();
                }
            }
        }
        public static void CalculateAndSaveQ4InDatabase(SqlConnection connection, int companyId)
        {
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
                foreach (var element in FinancialElementLists.HTMLElementsOfInterest)
                {
                    string columnName = element.Value.ColumnName;
                    bool isShares = element.Value.IsShares;
                    bool isCashFlowStatement = element.Value.IsCashFlowStatement;
                    bool isBalanceSheet = element.Value.IsBalanceSheet;
                    try
                    {   // Build the dynamic SQL update statement for Q4 calculation
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
                            command.Parameters.AddWithValue("@LeewayDays", 15);  // Allow 5 days leeway for matching dates
                            int rowsAffected = command.ExecuteNonQuery();
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                }
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
                }
            }
        }
        public static void CalculateQ4InDatabase(SqlConnection connection, int companyId)
        {
            foreach (var element in FinancialElementLists.ElementsOfInterest) // Iterate through each element in the FinancialElementLists.ElementsOfInterest
            {
                try
                {  // Build the dynamic SQL update statement for Q4 calculation
                    string q4CalculationQuery = $@"
            UPDATE FD4
            SET FD4.[{element}] = 
                (FD1.[{element}] - COALESCE(FD2.[{element}], 0) - COALESCE(FD3.[{element}], 0))
            FROM FinancialData FD1
            LEFT JOIN FinancialData FD2 ON FD1.CompanyID = FD2.CompanyID AND FD1.Year = FD2.Year AND FD2.Quarter = 1
            LEFT JOIN FinancialData FD3 ON FD1.CompanyID = FD3.CompanyID AND FD1.Year = FD3.Year AND FD3.Quarter = 2
            LEFT JOIN FinancialData FD4 ON FD1.CompanyID = FD4.CompanyID AND FD1.Year = FD4.Year AND FD4.Quarter = 4
            WHERE FD1.Quarter = 0 AND FD1.CompanyID = @CompanyID;";
                    using (SqlCommand command = new SqlCommand(q4CalculationQuery, connection))
                    {
                        command.Parameters.AddWithValue("@CompanyID", companyId);
                        int rowsAffected = command.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
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
        {  // Initialize the list to store batched SQL commands
            List<SqlCommand> batchedCommands = new List<SqlCommand>();
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
                                await HTML.HTML.ReparseHtmlReports(companyId, periodId, companyName, companySymbol, batchedCommands);
                            }
                            if (needsXbrlParsing)
                            {
                                await XBRL.XBRL.ReparseXbrlReports(companyId, periodId, companyName, companySymbol, batchedCommands);
                            }
                        }));
                    }
                    await Task.WhenAll(tasks);
                }
            }
            await ExecuteBatch(batchedCommands);
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
        public static async Task ExecuteBatch(List<SqlCommand> batchedCommands)
        {
            var batchTimer = Stopwatch.StartNew();
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
                            using (SqlConnection connection = new SqlConnection(Nasdaq100FinancialScraper.Program.connectionString))
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
            batchTimer.Stop();
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
