using Amazon.S3;
using Amazon.S3.Model;
using System.IO;
using System.Configuration;
using Newtonsoft.Json;

namespace DeeblyPlatform.Services
{
    public class S3Service : IS3Service
    {
        private readonly IAmazonS3 _s3;
        private readonly string bucketName;
        private readonly string awsRegion;
        public S3Service(IAmazonS3 s3, IConfiguration configuration)
        {
            _s3 = s3;
            bucketName = configuration["AWS:BucketName"];
            awsRegion = configuration["AWS:Region"];
        }
        public async Task<string> UploadImageToS3(string accountName, string campaignName, IFormFile file, int length = 0)
        {
            string validation = ValidateImage(file, length);
            if (validation != "valid") return validation;

            string key = $"{accountName}/{campaignName}/{file.FileName}";
            using Stream fileToUpload = file.OpenReadStream();

            PutObjectRequest request = new()
            {
                BucketName = bucketName,
                Key = key,
                InputStream = fileToUpload
            };

            PutObjectResponse response = await _s3.PutObjectAsync(request);

            await fileToUpload.DisposeAsync();

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK) return "An unknown error occured with the filesystem. Please email support@deebly.co.";

            return "Image uploaded successfully.";
        }

        public async Task<string> UploadAdBuilderRequestToS3(string accountName, string campaignName, FileStream file)
        {
            string fileName = Path.GetFileName(file.Name);
            string key = $"{accountName}/{campaignName}/{fileName}";

            PutObjectRequest request = new()
            {
                BucketName = bucketName,
                Key = key,
                InputStream = file
            };

            PutObjectResponse response = await _s3.PutObjectAsync(request);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK) return "An unknown error occured with the filesystem. Please email support@deebly.co.";

            return "Adset request successful.";

        }

        public async Task<bool> UploadAudienceToS3(string accountName, string campaignName, FileStream file)
        {
            string fileName = Path.GetFileName(file.Name);
            string key = $"{accountName}/{campaignName}/{fileName}";

            PutObjectRequest request = new()
            {
                BucketName = bucketName,
                Key = key,
                InputStream = file
            };

            PutObjectResponse response = await _s3.PutObjectAsync(request);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK) return false;

            return true;

        }        

        private string ValidateImage(IFormFile file, int length)
        {
            string message = "";

            string kb = (length / 1000).ToString();

            List<string> filetypes = new()
            {
                ".jpg",
                ".jpeg",
                ".png",
                ".gif"
            };

            string extension = Path.GetExtension(file.FileName).ToLower();

            if (string.IsNullOrEmpty(extension) || !filetypes.Contains(extension)) message += "This image is not in a supported file format. Please ensure your image is in either .jpg, .png, or .gif format. ";

            if (length > 0)
            {
                if (file.Length > length) message += "This image is over " + kb + " kb. Please compress your image and reupload. ";
            }

            if (message == "") message = "valid";

            return message;
        }

        public async Task<string> RemoveItemFromS3(string accountName, string campaignName, string fileName)
        {
            string key = $"{accountName}/{campaignName}/{fileName}";

            DeleteObjectRequest request = new()
            {
                BucketName = bucketName,
                Key = key
            };

            DeleteObjectResponse response = await _s3.DeleteObjectAsync(request);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.NoContent) return "Error: An unknown error occured with the filesystem. Please email support@deebly.co.";

            return "deleted";
        }

        public async Task<string> ClearFolder(string accountName, string campaignName)
        {
            string folderPath = $"{accountName}/{campaignName}/";

            ListObjectsV2Request listRequest = new()
            {
                BucketName = bucketName,
                Prefix = folderPath
            };

            ListObjectsV2Response listResponse = await _s3.ListObjectsV2Async(listRequest);

            List<KeyVersion> objects = new();

            foreach (S3Object thing in listResponse.S3Objects)
            {
                if (!thing.Key.EndsWith("audience.json"))
                {                    
                    KeyVersion key = new() { Key = thing.Key };

                    objects.Add(key);
                }

            }

            DeleteObjectsRequest request = new()
            {
                BucketName = bucketName,
                Objects = objects
            };

            DeleteObjectsResponse response = await _s3.DeleteObjectsAsync(request);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK) return "An unknown error occured with the filesystem. Please email support@deebly.co.";

            return "clear";

        }

        public async Task<List<string>> ListS3FolderContents(string accountName, string campaignName)
        {
            List<string> names = new();

            string folderPath = $"{accountName}/{campaignName}/";

            ListObjectsV2Request listRequest = new()
            {
                BucketName = bucketName,
                Prefix = folderPath
            };

            ListObjectsV2Response listResponse = await _s3.ListObjectsV2Async(listRequest);

            foreach (S3Object thing in listResponse.S3Objects)
            {
                names.Add(thing.Key.Replace(folderPath, ""));
            };

            return names;
        }

        public async Task<DateTime?> GetLastEditedDateFromS3(string accountName, string campaignName)
        {   
            DateTime? lastEditedDate = null;

            List<string> names = new();

            string folderPath = $"{accountName}/{campaignName}/";

            ListObjectsV2Request listRequest = new()
            {
                BucketName = bucketName,
                Prefix = folderPath
            };

            ListObjectsV2Response listResponse = await _s3.ListObjectsV2Async(listRequest);

            if(listResponse.S3Objects.Count > 0)
            {
                lastEditedDate = listResponse.S3Objects
                    .OrderByDescending(o => o.LastModified).Select(o => o.LastModified).FirstOrDefault();
            }

            return lastEditedDate;
        }

        public async Task<List<string>> GetSignedS3URLs(string accountName, string campaignName)
        {
            List<string> objectList = await ListS3FolderContents(accountName, campaignName);
            List<string> urls = new();

            foreach (string item in objectList)
            {
                if (item.EndsWith(".jpg") || item.EndsWith(".png") || item.EndsWith(".gif"))
                {
                    GetPreSignedUrlRequest request = new()
                    {
                        BucketName = bucketName,
                        Key = $"{accountName}/{campaignName}/{item}",
                        Expires = DateTime.Now.AddHours(1)
                    };

                    string url = _s3.GetPreSignedURL(request);

                    urls.Add(url);
                }

            }

            return urls;
        }

        public string GetSignedS3ObjectUrl(string accountName, string campaignName, string objectName)
        {
            GetPreSignedUrlRequest request = new()
            {
                BucketName = bucketName,
                Key = $"{accountName}/{campaignName}/{objectName}",
                Expires = DateTime.Now.AddHours(1)
            };

            string url = _s3.GetPreSignedURL(request);

            return url;
        }

        public async Task<string> GetAdBuilderJsonFromS3(string accountName, string campaignName)
        {
            string json = "";

            GetObjectRequest request = new()
            {
                BucketName = bucketName,
                Key = $"{accountName}/{campaignName}/ad_builder_request_{campaignName}.json"
            };

            GetObjectResponse res = await _s3.GetObjectAsync(request);

            using(Stream responseStream = res.ResponseStream) 
            {                
                StreamReader reader = new StreamReader(responseStream);

                json += await reader.ReadToEndAsync();
            }            
            

            return json;
        }

        public async Task<string> GetAudienceJsonFromS3(string accountName, string campaignName)
        {
            string json = "";

            bool exists = await CheckS3ForObject(accountName, campaignName, "audience.json");

            if (exists)
            {
                GetObjectRequest request = new()
                {
                    BucketName = bucketName,
                    Key = $"{accountName}/{campaignName}/audience.json"
                };

                GetObjectResponse res = await _s3.GetObjectAsync(request);


                using (Stream responseStream = res.ResponseStream)
                {
                    StreamReader reader = new StreamReader(responseStream);

                    json += await reader.ReadToEndAsync();
                }
            }            

            return json;
        }

        public string GetS3FolderURL(string accountName, string campaignName)
        {
            return $"https://s3.console.aws.amazon.com/s3/buckets/{bucketName}?region={awsRegion}&prefix={accountName}/{campaignName}/";
        }

        async Task<bool> CheckS3ForObject(string accountName, string campaignName, string fileName)
        {
            string folderPath = $"{accountName}/{campaignName}/";

            ListObjectsV2Request listRequest = new()
            {
                BucketName = bucketName,
                Prefix = folderPath
            };

            ListObjectsV2Response listResponse = await _s3.ListObjectsV2Async(listRequest);

            List<string> keys = listResponse.S3Objects.Select(o => o.Key).ToList();

            foreach (string key in keys)
            {
                if(key.Contains(fileName)) return true;
            }
            
            return false;
        }
    }
}
