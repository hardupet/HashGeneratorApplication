using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace HashGeneratorApplication.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MainHashController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly string _queueName;
        private readonly ConnectionFactory _factory;
        private readonly ILogger<MainHashController> _logger;
        public MainHashController(IConfiguration configuration, ILogger<MainHashController> logger)
        {
            _configuration = configuration;
            _queueName = "hashes";
            _factory = new ConnectionFactory() { HostName = "localhost" };
            _logger = logger; 
        }

        /// <summary>
        /// Hashes generated are sent one by one to RabbitMQ queue for further processing
        /// </summary>
        /// <returns></returns>
        [HttpPost("hashes/withoutBatching")]
        public IActionResult GenHashes()
        {
            try
            {
                var numOfHash = _configuration.GetValue<int>("AppSettings:NumOfHashes");
                using var connection = _factory.CreateConnection();
                using var channel = connection.CreateModel();

                // Generate SHA1 hashes
                var hashes = new List<string>();
                using (var sha1 = new SHA1Managed())
                {
                    for (var i = 0; i < numOfHash; i++)
                    {
                        var bytes = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
                        var hash = sha1.ComputeHash(bytes);
                        hashes.Add(BitConverter.ToString(hash).Replace("-", ""));
                    }
                }

                // Send each hash to RabbitMQ queue
                foreach (var hash in hashes)
                {
                    var body = Encoding.UTF8.GetBytes(hash);
                    channel.BasicPublish(exchange: "", routingKey: _queueName, basicProperties: null, body: body);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"An Error Occured {ex.Message}");
                return NotFound("An Error Occured, please try again");
            }
            

            return Ok();
        }

        /// <summary>
        /// Hashes generated are Splitted into batches and sent into RabbitMQ in parallel
        /// </summary>
        /// <returns></returns>
        [HttpPost("hashes")]
        public IActionResult GenerateHashes()
        {
            var numOfHash = _configuration.GetValue<int>("AppSettings:NumOfHashes");
            var amountInBatch = _configuration.GetValue<int>("AppSettings:AmountInBatch");
            using var connection = _factory.CreateConnection();
            using var channel = connection.CreateModel();

            // Generate SHA1 hashes
            var hashes = new List<string>();
            using (var sha1 = new SHA1Managed())
            {
                for (var i = 0; i < numOfHash; i++)
                {
                    var bytes = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
                    var hash = sha1.ComputeHash(bytes);
                    hashes.Add(BitConverter.ToString(hash).Replace("-", ""));
                }
            }

            // Split hashes into batches
            var batches = hashes.Select((hash, index) => new { Hash = hash, Index = index })
                                .GroupBy(x => x.Index / amountInBatch)
                                .Select(g => g.Select(x => x.Hash).ToList())
                                .ToList();

            // Send each batch of hashes to RabbitMQ queue in parallel
            Parallel.ForEach(batches, batch =>
            {
                foreach (var hash in batch)
                {
                    var body = Encoding.UTF8.GetBytes(hash);
                    channel.BasicPublish(exchange: "", routingKey: _queueName, basicProperties: null, body: body);
                }
            });
            string result = $" {numOfHash} Hashes generated with {amountInBatch} per batch and sent for processing Successfully";
            return Ok(result);
        }

        /// <summary>
        /// Get All Hashes from the Database without recalculating data on the fly using Stored Procedure
        /// </summary>
        /// <returns></returns>
        [HttpGet("hashes")]
        public async Task<IActionResult> GetHashesAsync()
        {
            //Connection to the Database
            var connectionString = _configuration.GetConnectionString("ConnectionString");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var command = new SqlCommand("sp_GetHashes", connection);
            command.CommandType = CommandType.StoredProcedure;

            //Execting the stored procedure
            using var reader = await command.ExecuteReaderAsync();

            var result = new List<object>();
            while (await reader.ReadAsync())
            {
                result.Add(new
                {
                    date = reader.GetDateTime(1).ToString("yyyy-MM-dd"),
                    count = reader.GetInt32(0)
                });
            }

            return Ok(new { hashes = result });
        }
    }
}

