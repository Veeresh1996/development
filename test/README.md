# pac-dataplatform-test

## Running Locally - ON Host

### Prerequisite

- use python 3.5 and above
- make sure mysql is running on host
- run the test_data_creator script to get the mocking data and output comparison json.

**Note:** The sql should be dumped in the local database
```
cd app/test/resource/helper;
python test_data_creator.py -d dbname
```

### Test

```
cd app/test;
python test_runner.py -v
```

### Prepare local data

```
mkdir -p data/input data/output data/scratch data/manifest
```

- Sample manifest files

**Note:** Download the files to `data/input` folder from s3

**file: data/manifest/local**
```
../data/input/daily_m25_2s_2020-02-16_06h30m_Sunday.sql.gz
../data/input/daily_m25_1q_2020-02-20_06h30m_Thursday.sql.gz
```

**Note:** The reason to use `../data` inside manifest file is because `make localrun` runs inside `app/` dir.

**file: data/manifest/local.docker**
```
/data/input/daily_m25_2s_2020-02-16_06h30m_Sunday.sql.gz
/data/input/daily_m25_1q_2020-02-20_06h30m_Thursday.sql.gz
```

**file: data/manifest/s3**
```
s3://pac-dl-lz-d27212f2-dbbb-4205-b980-aa1a7a394263/mascorp/mma/dbs/daily_m25_3j_2019-12-14_05h30m_Saturday.sql.gz
```

### Run

- Create .env File
```
WORKLOAD_INPUT_URI=../data/manifest/local
WORKLOAD_OUTPUT_PREFIX_S3URI=../data/output
TMP_DIR=../data/scratch
DO_DATA_LOAD=yes
SQL_IMPORT_METHOD=DEFAULT
START_MYSQL=no
LOG_LEVEL=INFO
DATA_DIR=/absolute/path/to/data
MYSQL_URL=mysql://root@localhost/mysql
```

- Run
```
make localrun
```

## Running Locally - via Docker

### Build Docker Image

```
make build
```

### Run

- Create docker.env File
```
WORKLOAD_INPUT_URI=/data/manifest/local.docker
WORKLOAD_OUTPUT_PREFIX_S3URI=/data/output
TMP_DIR=/data/scratch
LOG_LEVEL=INFO
AWS_BATCH_JOB_ID=1234567
```

- Run
```
make run
```
