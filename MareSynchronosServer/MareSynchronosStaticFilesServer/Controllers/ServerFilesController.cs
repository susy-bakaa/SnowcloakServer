using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text.Json;
using System.Text.RegularExpressions;
using Blake3;
using K4os.Compression.LZ4.Streams;
using MareSynchronos.API.Dto.Files;
using MareSynchronos.API.Routes;
using MareSynchronos.API.SignalR;
using MareSynchronosServer.Hubs;
using MareSynchronosShared.Data;
using MareSynchronosShared.Metrics;
using MareSynchronosShared.Models;
using MareSynchronosShared.Services;
using MareSynchronosShared.Utils.Configuration;
using MareSynchronosStaticFilesServer.Services;
using MareSynchronosStaticFilesServer.Utils;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Snowcloak.Files;

namespace MareSynchronosStaticFilesServer.Controllers;

[Route(MareFiles.ServerFiles)]
public class ServerFilesController : ControllerBase
{
    private static readonly SemaphoreSlim _fileLockDictLock = new(1);
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> _fileUploadLocks = new(StringComparer.Ordinal);
    private readonly string _basePath;
    private readonly string _coldBasePath;
    private readonly CachedFileProvider _cachedFileProvider;
    private readonly IConfigurationService<StaticFilesServerConfiguration> _configuration;
    private readonly IHubContext<MareHub> _hubContext;
    private readonly MareDbContext _mareDbContext;
    private readonly MareMetrics _metricsClient;

    public ServerFilesController(ILogger<ServerFilesController> logger, CachedFileProvider cachedFileProvider,
        IConfigurationService<StaticFilesServerConfiguration> configuration,
        IHubContext<MareHub> hubContext,
        MareDbContext mareDbContext, MareMetrics metricsClient) : base(logger)
    {
        _configuration = configuration;
        _basePath = configuration.GetValue<string>(nameof(StaticFilesServerConfiguration.CacheDirectory));
        if (_configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.UseColdStorage), false))
            _basePath = configuration.GetValue<string>(nameof(StaticFilesServerConfiguration.ColdStorageDirectory));
        _cachedFileProvider = cachedFileProvider;
        _hubContext = hubContext;
        _mareDbContext = mareDbContext;
        _metricsClient = metricsClient;
    }

    [HttpPost(MareFiles.ServerFiles_DeleteAll)]
    public async Task<IActionResult> FilesDeleteAll()
    {
        return Ok();
    }

    [HttpGet(MareFiles.ServerFiles_GetSizes)]
    public async Task<IActionResult> FilesGetSizes([FromBody] List<string> hashes)
    {
        var forbiddenFiles = await _mareDbContext.ForbiddenUploadEntries.
            Where(f => hashes.Contains(f.Hash)).ToListAsync().ConfigureAwait(false);
        List<DownloadFileDto> response = new();

        var cacheFile = await _mareDbContext.Files.AsNoTracking().Where(f => hashes.Contains(f.Hash)).AsNoTracking().Select(k => new { k.Hash, k.Size }).AsNoTracking().ToListAsync().ConfigureAwait(false);

        var allFileShards = new List<CdnShardConfiguration>(_configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.CdnShardConfiguration), new List<CdnShardConfiguration>()));

        foreach (var file in cacheFile)
        {
            var forbiddenFile = forbiddenFiles.SingleOrDefault(f => string.Equals(f.Hash, file.Hash, StringComparison.OrdinalIgnoreCase));
            Uri? baseUrl = null;

            if (forbiddenFile == null)
            {
                List<CdnShardConfiguration> selectedShards = new();
                var matchingShards = allFileShards.Where(f => new Regex(f.FileMatch).IsMatch(file.Hash)).ToList();

                if (string.Equals(Continent, "*", StringComparison.Ordinal))
                {
                    selectedShards = matchingShards;
                }
                else
                {
                    selectedShards = matchingShards.Where(c => c.Continents.Contains(Continent, StringComparer.OrdinalIgnoreCase)).ToList();
                    if (!selectedShards.Any()) selectedShards = matchingShards;
                }

                var shard = selectedShards
                    .OrderBy(s => !s.Continents.Any() ? 0 : 1)
                    .ThenBy(s => s.Continents.Contains("*", StringComparer.Ordinal) ? 0 : 1)
                    .ThenBy(g => Guid.NewGuid()).FirstOrDefault();

                baseUrl = shard?.CdnFullUrl ?? _configuration.GetValue<Uri>(nameof(StaticFilesServerConfiguration.CdnFullUrl));
            }

            response.Add(new DownloadFileDto
            {
                FileExists = file.Size > 0,
                ForbiddenBy = forbiddenFile?.ForbiddenBy ?? string.Empty,
                IsForbidden = forbiddenFile != null,
                Hash = file.Hash,
                Size = file.Size,
                Url = baseUrl?.ToString() ?? string.Empty,
            });
        }

        return Ok(JsonSerializer.Serialize(response));
    }

    [HttpPost(MareFiles.ServerFiles_FilesSend)]
    public async Task<IActionResult> FilesSend([FromBody] FilesSendDto filesSendDto)
    {
        var userSentHashes = new HashSet<string>(filesSendDto.FileHashes.Distinct(StringComparer.Ordinal).Select(s => string.Concat(s.Where(c => char.IsLetterOrDigit(c)))), StringComparer.Ordinal);
        var notCoveredFiles = new Dictionary<string, UploadFileDto>(StringComparer.Ordinal);
        var forbiddenFiles = await _mareDbContext.ForbiddenUploadEntries.AsNoTracking().Where(f => userSentHashes.Contains(f.Hash)).AsNoTracking().ToDictionaryAsync(f => f.Hash, f => f).ConfigureAwait(false);
        var existingFiles = await _mareDbContext.Files.AsNoTracking().Where(f => userSentHashes.Contains(f.Hash)).AsNoTracking().ToDictionaryAsync(f => f.Hash, f => f).ConfigureAwait(false);

        List<FileCache> fileCachesToUpload = new();
        foreach (var hash in userSentHashes)
        {
            // Skip empty file hashes, duplicate file hashes, forbidden file hashes and existing file hashes
            if (string.IsNullOrEmpty(hash)) { continue; }
            if (notCoveredFiles.ContainsKey(hash)) { continue; }
            if (forbiddenFiles.ContainsKey(hash))
            {
                notCoveredFiles[hash] = new UploadFileDto()
                {
                    ForbiddenBy = forbiddenFiles[hash].ForbiddenBy,
                    Hash = hash,
                    IsForbidden = true,
                };

                continue;
            }
            if (existingFiles.TryGetValue(hash, out var file) && file.Uploaded) { continue; }

            notCoveredFiles[hash] = new UploadFileDto()
            {
                Hash = hash,
            };
        }

        if (notCoveredFiles.Any(p => !p.Value.IsForbidden))
        {
            await _hubContext.Clients.Users(filesSendDto.UIDs).SendAsync(nameof(IMareHub.Client_UserReceiveUploadStatus), new MareSynchronos.API.Dto.User.UserDto(new(MareUser)))
                .ConfigureAwait(false);
        }

        return Ok(JsonSerializer.Serialize(notCoveredFiles.Values.ToList()));
    }

    [HttpPost(MareFiles.ServerFiles_Upload + "/{hash}")]
    [RequestSizeLimit(200 * 1024 * 1024)]
    public async Task<IActionResult> UploadFile(string hash, CancellationToken requestAborted)
    {
        _logger.LogInformation("{user} uploading file {file}", MareUser, hash);
        hash = hash.ToUpperInvariant();
        var existingFile = await _mareDbContext.Files.SingleOrDefaultAsync(f => f.Hash == hash);
        if (existingFile != null) return Ok();

        SemaphoreSlim? fileLock = null;
        bool successfullyWaited = false;
        while (!successfullyWaited && !requestAborted.IsCancellationRequested)
        {
            lock (_fileUploadLocks)
            {
                if (!_fileUploadLocks.TryGetValue(hash, out fileLock))
                    _fileUploadLocks[hash] = fileLock = new SemaphoreSlim(1);
            }

            try
            {
                await fileLock.WaitAsync(requestAborted).ConfigureAwait(false);
                successfullyWaited = true;
            }
            catch (ObjectDisposedException)
            {
                _logger.LogWarning("Semaphore disposed for {hash}, recreating", hash);
            }
        }

        try
        {
            var existingFileCheck2 = await _mareDbContext.Files.SingleOrDefaultAsync(f => f.Hash == hash);
            if (existingFileCheck2 != null)
            {
                return Ok();
            }

            _logger.LogInformation("[DEBUG] Receiving file {hash} from {user} at path {_basePath}", hash, MareUser, _basePath);
            var path = FilePathUtil.GetFilePath(_basePath, hash);
            var tmpPath = path + ".tmp";
            long compressedSize = -1;

            try
            {
                // ---
                // My own implementation of the new SCF file format upload process because the official one is not public yet.
                // Bit of a guess based on the SCFFile class and client but should work.
                // ---
                // 1. Save body as-is (SCF blob) to temp file
                await using (var tmpFileStream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    await Request.Body.CopyToAsync(tmpFileStream, requestAborted).ConfigureAwait(false);
                }

                // 2. Validate SCF header and hash
                await using (var scfStream = new FileStream(tmpPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    // This only parses header, does NOT decompress
                    var header = SCFFile.ReadHeader(scfStream);

                    // Sanity: header.Hash must match route param 'hash'
                    if (!string.Equals(header.Hash, hash, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new InvalidDataException($"SCF: Hash mismatch. Header={header.Hash}, Route={hash}");
                    }

                    // Optionally, we can also sanity-check compressed size vs file length:
                    var fileLength = scfStream.Length;
                    var expectedLength = (long)header.CompressedSize + 79; // 79-byte header
                    if (fileLength < expectedLength)
                    {
                        throw new InvalidDataException($"SCF: File too short. Length={fileLength}, Expected>={expectedLength}");
                    }

                    // For DB / metadata: treat compressedSize as total SCF size
                    compressedSize = fileLength;
                }

                // 3. Move verified SCF file into place
                System.IO.File.Move(tmpPath, path, true);
            }
            catch
            {
                try
                {
                    System.IO.File.Delete(tmpPath);
                }
                catch { /* ignore cleanup errors */ }

                throw;
            }

            // update on db
            await _mareDbContext.Files.AddAsync(new FileCache()
            {
                Hash = hash,
                UploadDate = DateTime.UtcNow,
                UploaderUID = MareUser,
                Size = compressedSize,
                Uploaded = true
            }).ConfigureAwait(false);
            await _mareDbContext.SaveChangesAsync().ConfigureAwait(false);

            _metricsClient.IncGauge(MetricsAPI.GaugeFilesTotal, 1);
            _metricsClient.IncGauge(MetricsAPI.GaugeFilesTotalSize, compressedSize);

            _fileUploadLocks.TryRemove(hash, out _);

            return Ok();
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error during file upload");
            return BadRequest();
        }
        finally
        {
            try
            {
                fileLock?.Release();
                fileLock?.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // it's disposed whatever
            }
            finally
            {
                _fileUploadLocks.TryRemove(hash, out _);
            }
        }
    }
}