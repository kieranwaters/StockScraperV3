using OpenQA.Selenium.Chrome;
using OpenQA.Selenium;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StockScraperV3
{
    public class XBRLElementData
    {
        private readonly string _connectionString;
        //
        public XBRLElementData(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task ParseAndSaveXbrlDataAsync()
        {
            // Configure ChromeDriver to run in headless mode for better performance and to avoid UI overhead
            var options = new ChromeOptions();
            options.AddArgument("--headless"); // Run Chrome in headless mode
            options.AddArgument("--disable-gpu"); // Applicable to Windows OS
            options.AddArgument("--no-sandbox"); // Bypass OS security model
            options.AddArgument("--disable-dev-shm-usage"); // Overcome limited resource problems

            using (var driver = new ChromeDriver(options))
            {
                try
                {
                    // Navigate to the target URL
                    driver.Navigate().GoToUrl("https://xbrl.us/data-rule/dqc_0015-le/");

                    // Locate all table rows using XPath
                    var rows = driver.FindElements(By.XPath("/html/body/div[4]/div/div[1]/div[1]/div/table/tbody/tr"));

                    using (var conn = new SqlConnection(_connectionString))
                    {
                        await conn.OpenAsync(); // Asynchronously open the connection

                        foreach (var row in rows)
                        {
                            var tds = row.FindElements(By.TagName("td")); // Get all <td> elements in the row

                            if (tds.Count >= 7) // Ensure there are enough columns
                            {
                                var elementLabel = tds[3].Text; // ElementLabel in td[4] (0-indexed)
                                var rawElementName = tds[4].Text; // RawElementName in td[5]
                                var balanceType = tds[5].Text; // BalanceType in td[6]
                                var definition = tds[6].Text; // Definition in td[7]

                                var query = "INSERT INTO XBRLDataTypes (ElementLabel, RawElementName, BalanceType, Definition) VALUES (@el, @re, @bt, @def)";

                                using (var cmd = new SqlCommand(query, conn))
                                {
                                    // Define parameters with appropriate types and sizes if possible
                                    cmd.Parameters.AddWithValue("@el", elementLabel ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue("@re", rawElementName ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue("@bt", balanceType ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue("@def", definition ?? (object)DBNull.Value);

                                    await cmd.ExecuteNonQueryAsync(); // Asynchronously execute the insert command
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Log the exception or handle it as per your application's requirement
                    Console.WriteLine($"[ERROR] An error occurred during parsing and saving XBRL data: {ex.Message}");
                    // Optionally, rethrow or handle the exception
                    throw;
                }
                finally
                {
                    // Ensure the browser is closed even if an exception occurs
                    driver.Quit();
                }
            }
        }
    }
}
