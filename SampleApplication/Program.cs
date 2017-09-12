using System;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.Diagnostics;

namespace SampleApplication
{
    class Program
    {

        private const string ENDPOINT_URI = "https://cosmospoc.documents.azure.com:443/";
        private const string PRIMARY_KEY = "jFya02h3XPehK1M5RDBUfUraXBnomaCHZjXGwro5auGYOFE4bqrDDXUK4AnCK9uIbVjHYBsNPcqaVw2JwXvmpg==";
        private const string DATABASE_NAME = "ReportingDB";
        private const string COLLECTION_NAME = "TransactionLog";

        private DocumentClient client;
        private long idData = DateTimeOffset.Now.Ticks;
        
        static void Main(string[] args)
        {
            try
            {
                Program prog = new Program();
                prog.InitiateClient().Wait();
            }
            catch (DocumentClientException ex)
            {
                Exception baseException = ex.GetBaseException();
                Console.WriteLine("{} error occured: {1}, Message: {2}", ex.StatusCode, ex.Message, baseException.Message);
            }
            catch(Exception ex)
            {
                Exception baseException = ex.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", ex.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("Exiting the app, press any key to exit.");
                Console.ReadKey();
            }
        }

        private async Task InitiateClient()
        {
            this.client = new DocumentClient(new Uri(ENDPOINT_URI), PRIMARY_KEY);
            //await this.client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DATABASE_NAME));
            await this.client.CreateDatabaseIfNotExistsAsync(new Database { Id = DATABASE_NAME });
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.Id = COLLECTION_NAME;
            myCollection.PartitionKey.Paths.Add("/CustomerName");

            //await this.client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(DATABASE_NAME), new DocumentCollection { Id = COLLECTION_NAME });
            await this.client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(DATABASE_NAME), myCollection);
            
            for (int i = 1; i <= 0; i++)
            {
                Log log1 = new Log
                {
                    Id = "Id" + idData,
                    ActivityDateTimeTicks = DateTimeOffset.Now.Ticks,
                    CRMCustomerId = "MicrosoftId",
                    CRMLicenseId = "L0-" + idData++,
                    CustomerId = 1,
                    CustomerName = "Microsoft",
                    DataFilePath = @"C:\Users\phinger1\Documents\Winshuttle\Studio\Data\VA02_20170720_142458.xlsx",
                    ErroredRecords = 0,
                    ManualUploadTime = "00:01:09",
                    Module = "Transaction",
                    RecordingMode = "Batch Input",
                    RecordsUploaded = 100,
                    RunReason = "For some reason",
                    SapClient = "800",
                    SapSystem = "W6R",
                    SapTcode = "MM02",
                    SapUsername = "phinger",
                    ScriptFilePath = @"C:\Users\phinger1\Documents\Winshuttle\Studio\Data\VA02_20170720_142458.txr",
                    ShuttleUploadTime = "00:00:02",
                    TimeSaved = "00:01:07",
                    TotalTimeSaved = "01:51:40",
                    UserEmail = "pritam.hinger@microsoft.com"
                };

                await this.CreateFamilyDocumentIfNotExists(DATABASE_NAME, COLLECTION_NAME, log1);

                Log log2 = new Log
                {
                    Id = "Id" + idData,
                    ActivityDateTimeTicks = DateTimeOffset.Now.Ticks,
                    CRMCustomerId = "GoogleId",
                    CRMLicenseId = "L0-" + idData++,
                    CustomerId = 1,
                    CustomerName = "Google",
                    DataFilePath = @"C:\Users\pritamh\Documents\Winshuttle\Studio\Data\VA02_20170720_142458.xlsx",
                    ErroredRecords = 0,
                    ManualUploadTime = "00:01:09",
                    Module = "Transaction",
                    RecordingMode = "Batch Input",
                    RecordsUploaded = 100,
                    RunReason = "For some reason",
                    SapClient = "800",
                    SapSystem = "W6R",
                    SapTcode = "MM02",
                    SapUsername = "pritamh",
                    ScriptFilePath = @"C:\Users\pritamh\Documents\Winshuttle\Studio\Data\VA02_20170720_142458.txr",
                    ShuttleUploadTime = "00:00:02",
                    TimeSaved = "00:01:07",
                    TotalTimeSaved = "01:51:40",
                    UserEmail = "pritam.hinger@google.com"
                };

                await this.CreateFamilyDocumentIfNotExists(DATABASE_NAME, COLLECTION_NAME, log2);
            }
            
            this.ExecuteSimpleQuery(DATABASE_NAME, COLLECTION_NAME);
            //await this.client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DATABASE_NAME));
        }

        private async Task CreateFamilyDocumentIfNotExists(string databaseName, string collectionName, Log log)
        {
            try
            {
                await this.client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, log.Id), new RequestOptions { PartitionKey = new PartitionKey("CustomerName")});
                this.WriteToConsoleAndPromptToContinue("Found {0}", log.Id);
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), log);
                    this.WriteToConsoleAndPromptToContinue("Created Log {0}", log.Id);
                }
                else
                {
                    Console.WriteLine("Error : {0}", ex.Message);
                    throw;
                }
            }
        }

        // ADD THIS PART TO YOUR CODE
        private void ExecuteSimpleQuery(string databaseName, string collectionName)
        {             
            // Set some common query options
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1};

            Stopwatch watch = new Stopwatch();
            watch.Start();
            //this.client.CreateDocumentQuery<Log>()
            // Here we find the Log via its UserEmail
            IQueryable<Log> logQuery = this.client.CreateDocumentQuery<Log>(
                    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), queryOptions, "CustomerName")
                    .Where(f => f.CustomerName == "Google");

            // The query is executed synchronously here, but can also be executed asynchronously via the IDocumentQuery<T> interface
            Console.WriteLine("Running LINQ query...");
            int i = 0;
            foreach (Log log in logQuery)
            {
                i++;
                Console.WriteLine("\tRead {0}", log);
            }

            Console.WriteLine("Total Count : " + i);
            watch.Stop();
            Console.WriteLine("Time taken : {0}", watch.ElapsedTicks);
            watch.Start();
            // Now execute the same query via direct SQL
            IQueryable<Log> logQueryInSql = this.client.CreateDocumentQuery<Log>(
                    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                    "SELECT * FROM Log WHERE Log.CustomerName = 'Google'",
                    queryOptions);

            Console.WriteLine("Running direct SQL query...");
            i = 0;
            foreach (Log log in logQueryInSql)
            {
                i++;
                Console.WriteLine("\tRead {0}", log);
            }

            Console.WriteLine("Total Count : " + i);
            watch.Stop();
            Console.WriteLine("Time taken : {0}", watch.ElapsedTicks);

            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

        // ADD THIS PART TO YOUR CODE
        private async Task DeleteFamilyDocument(string databaseName, string collectionName, string documentName)
        {
            try
            {
                await this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, documentName));
                Console.WriteLine("Deleted Family {0}", documentName);
            }
            catch (DocumentClientException ex)
            {
                Console.WriteLine("Error : {0}", ex.Message);
                throw;
            }
        }

        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            //Console.WriteLine("Press any key to continue ...");
            //Console.ReadKey();
        }

        public class Log
        {
            [JsonProperty(PropertyName ="id")]
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string CRMCustomerId { get; set; }
            public long CustomerId { get; set; }
            public string UserEmail { get; set; }
            public string SapUsername { get; set; }
            public string SapSystem { get; set; }
            public string SapClient { get; set; }
            public long ActivityDateTimeTicks { get; set; }
            public string ManualUploadTime { get; set; }
            public string ShuttleUploadTime { get; set; }
            public string TimeSaved { get; set; }
            public string TotalTimeSaved { get; set; }
            public string SapTcode { get; set; }
            public string  CRMLicenseId { get; set; }
            public string Module { get; set; }
            public string ScriptFilePath { get; set; }
            public string DataFilePath { get; set; }
            public int RecordsUploaded { get; set; }
            public int ErroredRecords { get; set; }
            public string RunReason { get; set; }
            public string RecordingMode { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
    }
}
