# Bucket Handler

This is a small cli tool and python library that handles querying, retrieving, and uploading files from bucket systems.

The main use case is for extremely large project files that do not need to persist on disk long term.

## Configuration

You must create a config.json file that contains your bucket API credentials. You can either pass in a custom file via `--config` or keep a `config.json` in your current working directory.

It is advised to first setup a test bucket until you get things working before using your production bucket. Be aware this library has the ability to write and remove files on your bucket if your credentials allow it.

```
{
	"account_key": "YourAccountKey",
	"application_key": "YourBucketApplicationKey",
	"bucket_name": "YourBucketName",
	"bucket_id": "YourBucketId"
}
```

# Usage

Note when specifying a "folder path" do not include a leading `/` as this is not how b2 works.
The root folder is just the empty set.

## Listing Files

```
bh ls "b2://sample_tests"
sample_tests/.bzEmpty
sample_tests/Screenshot 2025-08-06 164522.png
sample_tests/test.txt
```

If you don't want to get recursive files use `--norecurse` or set search(... recurse=False ... )

```
bh ls "b2://uploads_large/"
uploads_large/a.txt                             text/plain                      4.00B   2025-09-03 19:51:02
uploads_large/sample_tests/README.txt           text/plain                      22.00B  2025-08-25 16:46:26
uploads_large/sample_tests/figure.fbx           application/octet-stream        20.36MB 2025-08-26 11:09:50
uploads_large/sample_tests/large_file.zip       application/x-zip-compressed    1.56GB  2025-08-26 10:47:56
uploads_large/smaple_tests2/.bzEmpty            application/octet-stream        0.00B   2025-09-03 19:29:17
Total files: 5 examined, minTime: 2025-08-25 16:46:26 maxTime: 2025-09-03 19:51:02 time delta: 9:03:04:36
Duration: 0.71s
```

```
bh ls "b2://uploads_large/" --norecurse
uploads_large/a.txt             text/plain      4.00B   2025-09-03 19:51:02
uploads_large/sample_tests/
uploads_large/sample_tests2/
Total files: 3 examined, minTime: 2025-09-03 19:51:02 maxTime: 2025-09-03 19:51:02 time delta: 0:00:00:00
Duration: 0.60s
```

### Filters

When listing files you can use --filter for a regex and --maxsize and --minsize for file length restrictions.

```
bh ls 'b2://uploads_models' --filter='.*\.zip' --maxsize='500MB'
uploads_models/033064.zip          application/x-zip-compressed    164.34MB        2025-08-31 15:41:49
uploads_models/033065.zip          application/x-zip-compressed    180.39MB        2025-08-31 13:18:43
... snipped ...
uploads_models/070038.zip          application/x-zip-compressed    329.23MB        2025-08-31 12:13:17
uploads_models/071000.zip        application/x-zip-compressed    460.57MB        2025-08-31 12:14:46
Total files: 146 examined, minTime: 2025-08-30 21:25:48 maxTime: 2025-08-31 15:45:36 time delta: 0:18:19:48
Duration: 6.40s
```

## Downloading Files

Downloading files is accomplished by specifying one or more "input" paths followed by a single "output" path. By default it's always recursive.

Note: when downloading it will always create the same folder as the deepest ancestor unless you add "/*" (similar to cp -r).

```
bh cp "b2://downloads/"  ./sample_tests
./downloads/sample_tests/Screenshot+2025-08-06+164522.png
./downloads/sample_tests/test.txt
```

You can add a `--filter` which works the same as listing.

You can add a `--preservedir` flag to include the entire dir structure of the bucket.

You can also use `--threads` when downloading multiple files at once, which is very useful for many small files.

```
bh cp "b2://logs/2025/09/05/BIR_202509" /d/output/ --preservedir --filter=".*-r02l.*\.log" --threads=32
Downloaded [1]/[1787] => D:/output/logs/2025/09/05/BIR_20250905-001155-r02l-2606_012067_BATCH6.log
Downloaded [2]/[1787] => D:/output/logs/2025/09/05/BIR_20250905-000610-r02l-1602_012081_BATCH6.log
...
Downloaded [1786]/[1787] => D:/output/logs/2025/09/05/BIR_20250905-055206-r02l-1664_012301_BATCH6.log
Downloaded [1787]/[1787] => D:/output/logs/2025/09/05/BIR_20250905-054604-r02l-2638_012310_BATCH6.log
Duration: 29.77s
```

## Uploading Files

Use the `cp` command with one or more local "input" paths followed by one "output" path to upload to. By default it's always recursive.

```
bh cp ./downloads/sample_tests "b2://uploads2"
Uploaded downloads/sample_tests\Screenshot+2025-08-06+164522.png => uploads2/downloads/sample_tests/Screenshot+2025-08-06+164522.png
Uploaded downloads/sample_tests\test.txt => uploads2/downloads/sample_tests/test.txt
```

Large files on backblaze are also supported and are sped up with threads by splitting the uploads into multiple requests.

```
bh cp sample_tests/large_file.zip "b2://uploads_large"
Uploading file with key: sample_tests/large_file.zip
Uploading large file at  sample_tests/large_file.zip => uploads_large/sample_tests/large_file.zip chunks: 16 fileId: 4_zdb86658c7dba8ba4908c001e_f215f7102a9b31806_d20250905_m050558_c002_v0001151_t0050_u01757048758340
Uploading chunk 1/16 size: 104857600 bytes sha1: 517da6f455fbb12fa6fa37974cd4d9a311a62da7
Uploading chunk 2/16 size: 104857600 bytes sha1: 66dbe09681d316971c9323bd01e0ed7d8bb84fc2
Uploading chunk 3/16 size: 104857600 bytes sha1: 3e83d0c281a13d11e540031510cbd5624aeb32d0
Uploading chunk 4/16 size: 104857600 bytes sha1: e925c32fbb9cf896cabcac09d97d25924ad7773d
Uploading chunk 5/16 size: 104857600 bytes sha1: 0834524e330421446a43a8d0f780c76bdf15374e
Uploading chunk 6/16 size: 104857600 bytes sha1: a0e80105092204e0c024a7050395cec13486e39a
Uploading chunk 7/16 size: 104857600 bytes sha1: 5729626ff2fdd789d09f9a77a76b9fecb8a90a29
Uploading chunk 8/16 size: 104857600 bytes sha1: ddf4a965a6cbe21addd450dc234881a908d9f387
Uploading chunk 9/16 size: 104857600 bytes sha1: 0f84037f87035b062bc74957b27f0a8988ee1672
Uploading chunk 10/16 size: 104857600 bytes sha1: 0c07345377b982cc0dbb30ecbc32cb6fe3c568e3
Uploading chunk 11/16 size: 104857600 bytes sha1: 20f7f611a8495e4664c027675499cafb1ef4f5d9
Uploading chunk 12/16 size: 104857600 bytes sha1: eab206ce3c96ec62b4e690915bdae7130522868b
Uploading chunk 13/16 size: 104857600 bytes sha1: 4f24310903b02a76529ebf35d5c99b9cd238e1c3
Uploading chunk 14/16 size: 104857600 bytes sha1: 89bf702bab5b61f154bb5ca90aee7be47bbf48a1
Uploading chunk 15/16 size: 104857600 bytes sha1: cf404f765d79d89e4762584956ca63148852a9bb
Uploading chunk 16/16 size: 100397009 bytes sha1: 6dcf5919ad16e1063fa1b4b77ba41a5c8b419091
Tripping finish for file: 4_zdb86658c7dba8ba4908c001e_f215f7102a9b31806_d20250905_m050558_c002_v0001151_t0050_u01757048758340 with ['517da6f455fbb12fa6fa37974cd4d9a311a62da7', '66dbe09681d316971c9323bd01e0ed7d8bb84fc2', '3e83d0c281a13d11e540031510cbd5624aeb32d0', 'e925c32fbb9cf896cabcac09d97d25924ad7773d', '0834524e330421446a43a8d0f780c76bdf15374e', 'a0e80105092204e0c024a7050395cec13486e39a', '5729626ff2fdd789d09f9a77a76b9fecb8a90a29', 'ddf4a965a6cbe21addd450dc234881a908d9f387', '0f84037f87035b062bc74957b27f0a8988ee1672', '0c07345377b982cc0dbb30ecbc32cb6fe3c568e3', '20f7f611a8495e4664c027675499cafb1ef4f5d9', 'eab206ce3c96ec62b4e690915bdae7130522868b', '4f24310903b02a76529ebf35d5c99b9cd238e1c3', '89bf702bab5b61f154bb5ca90aee7be47bbf48a1', 'cf404f765d79d89e4762584956ca63148852a9bb', '6dcf5919ad16e1063fa1b4b77ba41a5c8b419091']
Duration: 41.05s
```

## Removing Files

Use the `rm` command to remove files. For safety, by default recursion is disabled and you must add `--recurse` to do so.

By default all versions of a file will be removed, to delete only the latest version use `--latestonly`.

```
bh rm "b2://bar" --recursive
Deleted bar/car/sim/a.txt
Deleted bar/car/sim/b.txt
Deleted bar/car/sim/micro.zip
Deleted bar/car/sim/nested/a/b/c.txt
Deleted bar/car/sim/sample_data.zip
Deleted bar/car/sim/sample_data_uncompressed.zip
Deleted bar/car/sim/test.zip
Deleted 7 files
```


## Installing

If you are installing this from source and not pip, you can use `pip install -e .` in the buckethandler folder to install it.
