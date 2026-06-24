# Bucket Handler

This is a small cli tool and python library that handles querying, retrieving, and uploading files from bucket systems.

The main use case is for extremely large project files that do not need to persist on disk long term.

## Supported Buckets and Features

| System        | Search | Upload | Download | Move | Remove | DL URL | UL URL |
| ------------- | ------ | ------ | -------- | ---- | ------ | ------ | ------ |
| Backblaze B2  | Y      | Y      | Y        | N    | Y      | Y      | N      |
| Amazon S3     | Y      | Y      | Y        | N    | N      | Y      | N      |
| Dropbox       | Y      | Y      | Y        | N    | N      | Y      | N      |

## Installation

To get access to local and http/https files you can run:
```
pip install buckethandler
```

If you need dropbox support you will need to install the optional dropbox requirements. S3 and B2 support is custom and builtin.
```
pip install buckethandler[dropbox]
```
or simply
```
pip install dropbox
```

If you don't want to use pip and instead want to install locally via this copy (or want to contribute development) you can run the following after cloning and cd'ing into the repository.

```
pip install -e .
```

## Configuration

You can either set environment variables, a config dictionary object, or a JSON file.

### cli

It is advised to first setup a test bucket until you get things working before using your production bucket. Be aware this library has the ability to write and remove files on your bucket if your credentials allow it.

### Config Choices

| Key              | Note                                                   | Backblaze      | AWS S3                     | Dropbox       |
| ---------------- | -------------------------------------------------------| -------------- | -------------------------- | ------------- |
| BH_PUBLIC_KEY    | The publicly visible key or app id                     | keyID          | Access key                 | App key       |
| BH_SECRET_KEY    | A secret key, usually given once on creation           | applicationKey | Secret access key          | App secret    |
| BH_BUCKET_NAME   | The human readable name given to the bucket            | Bucket         | Bucket                     | &#10060;      |
| BH_BUCKET_ID     | The generated id given to the bucket                   | Bucket Id      | &#10060;                   | &#10060;      |
| BH_REGION_ID     | The region for the bucket, eg: us-west-2               | Endpoint[^2]   | AWS Region                 | &#10060;      |
| BH_ACCESS_TOKEN  | Created with the *--authorize* flag                    | &#10060;       | &#10060;                   | access token  |
| BH_REFRESH_TOKEN | Created with the *--authorize* flag                    | &#10060;       | &#10060;                   | refresh token |
| BH_B2_AS_S3      | Set true if you want to emulate a B2 bucket via S3[^3] |                | &#10060;                   | &#10060;      |

[^1]: Blank values can be ignored and don't need to be specified in the env/config.
[^2]: Backblaze does not need a region unless you are using the S3 interface for B2.
[^3]: Backblaze supports accessing B2 via an S3 interface, this is supported but not recommended, use the B2 handler instead.

### Backblaze
You will need to login to your backblaze admin panel or use the b2 cli.

1. **BH_ACCOUNT_KEY** is from the *Application Key* you create, it's referenced in B2 as *keyID*.
2. **BH_APPLICATION_KEY** is from the *Application Key* you create, this is the "secret" key that is the longer key, it is longer than the account BH_ACCOUNT_KEY and must be downloaded when you create they key or it's lost. It's shown to you once as *applicationKey*.
3. **BH_BUCKET_NAME** is the actual name you gave the bucket, it appears as the title of the bucket name cards after you make one.
4. **BH_BUCKET_ID** comes from the bucket you create. It will be an alphanumeric id, not the name you give it. You get this id in the admin interface that lists the buckets as *Bucket ID* or through the actual b2 cli.

Note: the **BH_REGION_ID** is not used for B2 unless you are using it via S3 (not recommended).

### S3

You will need to login to your aws console or use the s3 cli. The S3 key comes from IAM access, so you can create an IAM policy for S3. Typically you'll create a custom policy for S3 and assign it to your user. The custom policy might look something like this (policies -> create policy -> specify permissions -> s3 -> switch from visual to JSON):

~~~json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "Statement1",
			"Effect": "Allow",
			"Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
			"Resource": [
                "arn:aws:s3:::MY-BUCKET-NAME",
                "arn:aws:s3:::MY-BUCKET-NAME/*"
            ]
		}
	]
}
~~~

It's normal to give this to a group first, then assign this group to your user(s). Once you have your user, your S3 bucket, and your policy you can get your access key. Go to the user in the IAM users page, and click "create access key", choose "Other", then give it a name and description. You'll be then presented at a page where you must copy you "Secret access key" or you will lose it. You will also need the "Access key". THe "Secret access key" will be longer than the "Access key". Then hit done.

Note: While possible to access a B2 bucket via the B2 S3 interface, it's not recommended. If you want to use it, set **BH_B2_AS_S3** to true.

### Dropbox

Dropbox is a little trickier, it has a few methods for getting access, but currently Bucket Handler only supports one.

You must create a Dropbox App via https://www.dropbox.com/developers/apps. Make it a Development status with permissions set to Scoped App it must have:

1. account_info.read
1. files.metadata.write
1. files.metadata.read
1. files.content.write
1. files.content.read
1. sharing.write
1. sharing.read

Get the *App key* and show the *App secret* and copy both of these. After you have these you then call

```
bh authorize --dropbox
```

With whatever suitable config you created (env/json/etc). It will launch a browser or give you a url to visit and instructions for getting both the refresh token and access token. You must then take these an add them to your configuration for the **BH_ACCESS_TOKEN** and **BH_REFRESH_TOKEN**.


### Backblaze B2
b2 sample config
```
{
	"BH_PUBLIC_KEY": "...",
	"BH_SECRET_KEY": "...",
	"BH_BUCKET_NAME": "...",
	"BH_BUCKET_ID": "...",
}
```

### Amazon S3
s3 sample config
```
{
	"BH_PUBLIC_KEY": "...",
	"BH_SECRET_KEY": "...",
	"BH_BUCKET_NAME": "...",
	"BH_BUCKET_ID": "...",
	"BH_REGION_ID": "...",
}
```

### Dropbox
db sample config
```
{
	"BH_PUBLIC_KEY": "...",
	"BH_SECRET_KEY": "...",
	"BH_ACCESS_TOKEN": "...",
	"BH_REFRESH_TOKEN": "...",
}
```

Note, you can get the `dropbox_access_token` and `dropbox_refresh_token` by triggering the `bh authorize --dropbox --config=YourConfig.json` argument to the CLI interface. Your `dropbox_app_key` and `dropbox_secret` come from creating an application in dropbox account settings.

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
uploads_models/071000.zip          application/x-zip-compressed    460.57MB        2025-08-31 12:14:46
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
