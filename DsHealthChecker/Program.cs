using Polly;
using System.Data.SqlClient;
using System.Diagnostics;

namespace HealthCheck
{
    // From: https://techcommunity.microsoft.com/t5/azure-database-support-blog/lesson-learned-449-unleashing-concurrent-threads-for-robust/ba-p/3966484
    // Refactor and code tidy

    public class Program
    {
        const string connectionString = "data source=tcp:servername.database.windows.net,1433;initial catalog=dname;User ID=username;Password=password;ConnectRetryCount=3;ConnectRetryInterval=10;Connection Timeout=30;Max Pool Size=1200;MultipleActiveResultSets=false;Min Pool Size=1;Application Name=Testing by JMJD - SQL;Pooling=true";
        const string LogFilePath = "c:\\temp\\log.txt";

        // The query to execute, this should be something useful for your database to test the connection
        const string Query = "SELECT 1";
        const int CommandTimeout = 5;

        static async Task Main(string[] args)
        {
            int numberOfThreads = 15000; //Nr Threads
            int maxDegreeOfParallelism = 850; //Nr Threads to run concurrent

            var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);

            var tasks = new Task[numberOfThreads];
            for (var i = 0; i < numberOfThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    await semaphore.WaitAsync();
                    try
                    {
                        await ExecuteQueryAsync(connectionString);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                });
            }

            await Task.WhenAll(tasks);
        }

        private static async Task ExecuteQueryAsync(string connectionString)
        {
            var threadId = Environment.CurrentManagedThreadId;
            TimeSpan ts;
            string elapsedTime;
            try
            {
                var stopWatch = new Stopwatch();
                stopWatch.Start();

                Log($"Thread {threadId}: Started");
                Log($"Thread {threadId}: Opening the connection");

                var connection = await ConnectWithRetriesAsync(connectionString);

                ts = stopWatch.Elapsed;

                elapsedTime = FormatElapsedTime(ts);

                Log($"Thread {threadId}: Connected - {elapsedTime} " + connection.ClientConnectionId.ToString());
                Log($"Thread {threadId}: Executing the command");

                var command = new SqlCommand(Query, connection)
                {
                    CommandTimeout = CommandTimeout
                };

                stopWatch.Reset();
                stopWatch.Start();

                var result = await ExecuteCommandWithRetriesAsync(command);

                stopWatch.Stop();
                ts = stopWatch.Elapsed;

                elapsedTime = FormatElapsedTime(ts);

                Log($"Thread {threadId}: Executed the command - {elapsedTime} - Result: {result}");
                Log($"Thread {threadId}: Closing the connection");

                connection.Close();
            }
            catch (OperationCanceledException operationCancelledException)
            {
                Log($"Thread {threadId}: Error (Cancelation): {operationCancelledException.Message}");
            }
            catch (Exception ex)
            {
                Log($"Thread {threadId}: - Error (Exception): {ex.Message}");
            }
        }

        private static async Task<SqlConnection> ConnectWithRetriesAsync(string connectionString)
        {
            var connection = new SqlConnection(connectionString);

            var policy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(connection.ConnectionTimeout * 1.05),
                    (exception, timespan, retryCount, context) =>
                    {
                        Log($"Retry {retryCount} due to {exception.Message}. Will retry in {timespan.TotalSeconds} seconds.");
                    });

            await policy.ExecuteAsync(async () =>
            {
                try
                {
                    await connection.OpenAsync();
                }
                catch (Exception)
                {
                    throw;
                }
            });

            return connection;
        }

        private static async Task<object> ExecuteCommandWithRetriesAsync(SqlCommand command)
        {
            var policy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(command.CommandTimeout * 1.05),
                    (exception, timespan, retryCount, context) =>
                    {
                        Log($"Retry {retryCount} due to {exception.Message}. Will retry in {timespan.TotalSeconds} seconds.");
                    });

            object? result = null;
            await policy.ExecuteAsync(async () =>
            {
                try
                {
                    result = await command.ExecuteScalarAsync();
                }
                catch (Exception)
                {
                    throw;
                }
            });

            return result!;
        }

        private static void Log(string message)
        {
            string logMessage = $"{DateTime.Now}: {message}";
            Console.WriteLine(logMessage);
            try
            {
                using var stream = new FileStream(LogFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
                using var writer = new StreamWriter(stream);
                
                writer.WriteLine(logMessage);
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error writing in the log file: {ex.Message}");
            }
        }

       private static string FormatElapsedTime(TimeSpan ts) =>
            string.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
    }
}