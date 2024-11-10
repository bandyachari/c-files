using System.Collections.Generic;
using System.Threading.Tasks;
using Neighborly.GoogleDirectBooking.API.Application.Services.Feeds.ViewModels;
using Neighborly.GoogleDirectBooking.API.Application.Services.FullAttribute.Brokers;
using Neighborly.GoogleDirectBooking.API.Application.Services.FullAttribute.ViewModels;
using Neighborly.GoogleDirectBooking.API.Domain.Constants;
using Newtonsoft.Json;
using System;
using Neighborly.GoogleDirectBooking.API.Domain.Interfaces.Services;
using AutoMapper;
using System.Linq;
using Neighborly.GoogleDirectBooking.API.Application.Services.UrlTransformer.Brokers;
using Serilog;
using System.IO;
using System.IO.Compression;
using Neighborly.GoogleDirectBooking.API.Application.Services.UrlTransformer.ViewModels;
using System.Data;
using System.Text;
using Microsoft.Extensions.Options;
using Neighborly.GoogleDirectBooking.API.Domain.Settings;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.DataProtection.KeyManagement;


namespace Neighborly.GoogleDirectBooking.API.Application.Services.Feeds
{
    public class FeedService : IFeedService
    {

        private readonly IConfiguration _configuration;
        private readonly IFullAttributeService _fullAttributeService;
        private readonly IS3FileUploadService _s3FileUploadService;
        private readonly AwsSettings _awsSettings;
        private readonly IMapper _mapper;
        private readonly IUrlTransformerService _urlTransformerService;
        private readonly ILogger _logger;

        public FeedService(IConfiguration configuration, IFullAttributeService fullAttributeService, IMapper mapper, IUrlTransformerService urlTransformerService, IS3FileUploadService s3FileUploadService, IOptions<AwsSettings> awsSettings, ILogger logger)
        {
            _configuration = configuration;
            _fullAttributeService = fullAttributeService;
            _mapper = mapper;
            _urlTransformerService = urlTransformerService;
            _s3FileUploadService = s3FileUploadService;
            _awsSettings = awsSettings.Value;
            _logger = logger;
        }

        public async Task<string> GenerateFeed(string apiKey, string apiVersion, List<string> brandsList, string correlationId)
        {

            List<BrandsModel> usbrandsJsonList;
            List<BrandsModel> cabrandsJsonList;

            var brandConfigSection = _configuration.GetSection(Constants.BrandConfig);
            var brands = brandConfigSection.Get<Dictionary<string, BrandDetails>>();

            if (brandsList != null && brandsList.Count != 0)
            {
                brands = brands.Where(x => brandsList.Contains(x.Key)).ToDictionary(k => k.Key, v => v.Value);
            }

            try
            {
                usbrandsJsonList = await _urlTransformerService.GetBrandsJsonData(Constants.USBrands);
                cabrandsJsonList = await _urlTransformerService.GetBrandsJsonData(Constants.CABrands);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"{this.GetType().FullName}.GenerateFeed - Error occured while fetching the brand json {ex.StackTrace} {ex.Message}");
                _logger.Error($"{this.GetType().FullName}.GenerateFeed - Job failed");

                return Constants.JobFailed;
            }

            foreach (var brand in brands)
            {

                var allEntities = new GenericFeed<Entity>();
                var allActions = new GenericFeed<Actions>();
                PopulateEntities(brand, usbrandsJsonList, cabrandsJsonList, allEntities, allActions, apiKey, apiVersion, correlationId);


                _logger.Information($"{this.GetType().FullName}.GenerateFeed - Completed the feed generation for {brand.Key} brand");

                var folderName = _awsSettings.FolderName;
                var bucketName = _awsSettings.S3Bucket;

                try
                {
                    using var entityGzipStream = ConvertJsonToGZip(JsonConvert.SerializeObject(allEntities));
                    await _s3FileUploadService.UploadToS3Async(entityGzipStream, bucketName, folderName, GetS3Key(brand.Key, Constants.EntityFeed));

                    _logger.Information($"{this.GetType().FullName}.GenerateFeed - Uploading to S3 is completed for entity feed.");


                    using var actionGzipStream = ConvertJsonToGZip(JsonConvert.SerializeObject(allActions));
                    await _s3FileUploadService.UploadToS3Async(actionGzipStream, bucketName, folderName, GetS3Key(brand.Key, Constants.ActionFeed));
                    _logger.Information($"{this.GetType().FullName}.GenerateFeed - Uploading to S3 is completed for action feed.");
                }
                catch (Exception ex)
                {
                    _logger.Error($"{this.GetType().FullName}.GenerateFeed - Uploading to S3 is failed for {brand.Key} brand {ex.StackTrace} {ex.Message}");
                }
            }

            _logger.Information(this.GetType().FullName, "Job Completed");
            return Constants.SuccessStatusMessage;
        }

        private async Task<(bool Success, Entity Entity, Actions Action)> TryGetFullAttributeWithRetry(int location, string brandKey, string apiKey, string apiVersion, string correlationId,
            List<BrandsModel> usbrandsJsonList,
            List<BrandsModel> cabrandsJsonList)
        {
            _logger.Debug($"{this.GetType().FullName}.TryGetFullAttributeWithRetry - Attempting to get full attribute for location: {location}, brand: {brandKey}.");

            int retryCount = 0;

            while (retryCount < Constants.MaxRetries)
            {
                try
                {

                    var response = await _fullAttributeService.GetFullAttribute(location, apiKey, apiVersion, correlationId);

                    if (response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        if (!string.IsNullOrEmpty(content))
                        {
                            var franchiseFullAttributeResponse = JsonConvert.DeserializeObject<FullAttributeEntity>(content);

                            var result = ProcessSuccessResponse(franchiseFullAttributeResponse, brandKey, usbrandsJsonList, cabrandsJsonList);

                            return (true, result.Entity, result.Action);
                        }
                        else
                        {
                            _logger.Warning($"{this.GetType().FullName}.TryGetFullAttributeWithRetry - Received empty content for location: {location}.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Warning(ex, $"{this.GetType().FullName}.TryGetFullAttributeWithRetry - Error during FullAttribute API call for location {location}. Attempt {retryCount + 1}. {ex.StackTrace} {ex.Message}");
                }

                retryCount++;
                if (retryCount == Constants.MaxRetries)
                {
                    _logger.Error($"{this.GetType().FullName}.TryGetFullAttributeWithRetry - FullAttribute API failed for location {location} after {Constants.MaxRetries} attempts.");
                }
            }

            return (false, null, null);
        }
        private (Entity Entity, Actions Action) ProcessSuccessResponse(FullAttributeEntity franchiseFullAttributeResponse, string brandKey,
            List<BrandsModel> usbrandsJsonList,
            List<BrandsModel> cabrandsJsonList)
        {
            _logger.Debug($"{this.GetType().FullName}.ProcessSuccessResponse - Processing success response for brand: {brandKey}.");

            var entity = _mapper.Map<Entity>(franchiseFullAttributeResponse);

            var country = entity.Location.Address.Country;

            var action = CreateAction(franchiseFullAttributeResponse, country, brandKey, usbrandsJsonList, cabrandsJsonList);

            _logger.Debug($"{this.GetType().FullName}.ProcessSuccessResponse - Processed action for brand: {brandKey}, location: {entity.Location.Address.Region}.");

            return (entity, action);
        }

        private Actions CreateAction(FullAttributeEntity franchiseFullAttributeResponse, string country, string brandKey,
            List<BrandsModel> usbrandsJsonList,
            List<BrandsModel> cabrandsJsonList)
        {
            _logger.Debug($"{this.GetType().FullName}.CreateAction - Creating action for brand: {brandKey}  and country: {country}.");

            var brandBaseURL = country.Equals(Constants.USStateCode)
                ? usbrandsJsonList.First(x => x.BrandCode == brandKey).BookingUrl.TrimEnd('/')
                : cabrandsJsonList.First(x => x.BrandCode == brandKey).BookingUrl.TrimEnd('/');

            var action = new Actions
            {
                EntityId = franchiseFullAttributeResponse.FranchiseWebLocationId.ToString(),
                LinkID = Constants.AppointmentLink + franchiseFullAttributeResponse.FranchiseWebLocationId,
                Url = $"{brandBaseURL}&location={franchiseFullAttributeResponse.FranchiseWebLocationId}",
                Action = new List<ViewModels.Action>
                {
                    new ViewModels.Action
                    {
                        AppointmentInfo = new AppointmentInfo
                        {
                            Url = $"{brandBaseURL}&location={franchiseFullAttributeResponse.FranchiseWebLocationId}"
                        }
                    }
                }
            };

            _logger.Debug($"{this.GetType().FullName}.CreateAction - Created action with URL: {action.Url} for brand: {brandKey}.");
            return action;
        }



        public static MemoryStream ConvertJsonToGZip(string JsonData)
        {

            var memoryStream = new MemoryStream();
            byte[] jsonBytes = Encoding.UTF8.GetBytes(JsonData);

            using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
            {
                gzipStream.Write(jsonBytes, 0, jsonBytes.Length);
            }

            return memoryStream;
        }

        private static string GetS3Key(string brandKey, string type)
        {
            string[] feedType = type.Split("-");
            string fileName = feedType[0];

            return $"{brandKey}/{fileName}.json.gz";
        }

        private async Task PopulateEntities(
                        KeyValuePair<string, BrandDetails> brand,
                        List<BrandsModel> usbrandsJsonList,
                        List<BrandsModel> cabrandsJsonList,
                        GenericFeed<Entity> allEntities,
                        GenericFeed<Actions> allActions,
                        string apiKey,
                        string apiVersion,
                        string correlationId
        )
        {

            _logger.Information($"{this.GetType().FullName}.GenerateFeed - Started the feed generation for {brand.Key} brand");
            var brandKey = brand.Key;
            var brandDetails = brand.Value;

            int consecutiveFailures = 0;
            foreach (var webLocation in brandDetails.WebLocations.Distinct())
            {
                var location = Convert.ToInt32(webLocation);


                var result = await TryGetFullAttributeWithRetry(location, brandKey, apiKey, apiVersion, correlationId, usbrandsJsonList, cabrandsJsonList);

                if (result.Success)
                {
                    allEntities.Data.Add(result.Entity);
                    allActions.Data.Add(result.Action);
                    consecutiveFailures = 0;

                }
                else
                {
                    consecutiveFailures++;
                    if (consecutiveFailures >= Constants.MaxConsecutiveFailures)
                    {
                        _logger.Error($"{this.GetType().FullName}.GenerateFeed - FullAttribute API failed for {Constants.MaxConsecutiveFailures} consecutive locations. Skipping job for {brandKey} brand.");
                        break;
                    }
                }
            }

        }
    }
}

