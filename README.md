# HMS Mirror and Ozone Migration Automation

A comprehensive toolkit for automating Hive Metastore migrations from HDFS to Ozone storage using HMS Mirror, with support for volume/bucket creation and database migration workflows.

## Overview

This project provides automation scripts and tools to facilitate the migration of Hive databases from HDFS to Ozone storage systems. It includes utilities for:

- Creating Ozone volumes and buckets based on HDFS FID directories
- Automating HMS Mirror execution for database migrations
- Identifying and excluding tables with non-standard locations
- Post-processing SQL scripts for clean deployment

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [HMS Mirror Setup](#hms-mirror-setup)
- [Ozone Volume and Bucket Creation](#ozone-volume-and-bucket-creation)
- [HMS Mirror Automation](#hms-mirror-automation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Known Limitations](#known-limitations)

## Prerequisites

### Software Requirements

- **Java Development Kit (JDK) 17** - Required for HMS Mirror
- **Python 3.6 or higher**
- **MySQL Client** - For metastore database access
- **Bash 4.0+** - For SQL processing scripts
- **HMS Mirror 3.1.0.1** or compatible version

### Python Dependencies

```bash
# Check if requests module is installed
python -c "import requests; print('requests is installed, version:', requests.__version__)"

# Install if not present
pip install requests
# OR
python -m pip install requests
```

### Network Access

- MySQL database connectivity (Hive Metastore)
- HDFS/Ozone storage access
- Kerberos authentication (if applicable)

### Environment Setup

```bash
export JAVA_HOME=/opt/jdk-17
export PATH=$JAVA_HOME/bin:$PATH
```

## Installation

### 1. HMS Mirror Installation

HMS Mirror must be installed as root user under `/root` directory.

```bash
# Download HMS Mirror

# Extract tarball
tar zxvf hms-mirror-3.1.0.1-dist.tar.gz

# Run installation script as root
sudo hms-mirror-install-3.1.0.1/setup.sh
```

This installs HMS Mirror packages in `/usr/local/hms-mirror` and creates symlinks in `/usr/local/bin`.

**Important:** Do NOT run hms-mirror from the installation directory.


### 3. Kerberos Authentication

If your environment uses Kerberos:

```bash
kinit -kt /path/to/hive.keytab <hive-principal>
```

## HMS Mirror Setup

### Initial Configuration

```bash
# Set Java environment
export JAVA_HOME=/opt/jdk-17
export PATH=$JAVA_HOME/bin:$PATH

# Run HMS Mirror setup wizard
hms-mirror --setup
```

During setup, you'll be prompted to configure:

- **LEFT cluster**: Source cluster namespace, JDBC URI, platform type
- **RIGHT cluster**: Target cluster namespace, JDBC URI, platform type
- Kerberos settings (if applicable)
- Hive standalone JAR location
- Metastore direct link (optional but recommended)

### Configuration Files

After initial setup, configuration files are stored in:
```
/root/.hms-mirror/cfg/
```

Copy the provided YAML templates to this directory:
- `2025-11-12_23-02-06_Storage_Migration.yaml`
- `2025-11-13_00-03-02_schema_only_latest_nov13.yaml`

### Update Configuration Parameters

Edit the YAML files with your cluster-specific settings:

```yaml
hiveServer2:
  uri: "jdbc:hive2://<host>:<port>/default;principal=hive/_HOST@<REALM>;ssl=true;..."
  jarFile: "/path/to/hive-jdbc-standalone.jar"
  version: "3.1.3000.7.1.9.1032-3"

metastore_direct:
  uri: "jdbc:mysql://<host>:3306/<database>"
  type: "MYSQL"
  resource: "/root/.hms-mirror/aux_libs/mysql-connector-java.jar"
  connectionProperties:
    user: "<username>"
    password: "<password>"
```

**Note:** Use Thrift URL instead of ZooKeeper for hiveServer2 URI.

### Start HMS Mirror Service

```bash
# Start with default port (8090)
hms-mirror --service

# Or specify custom port
hms-mirror --service --server.port=8095
```

Access the web interface at: `http://<host>:8090/hms-mirror`

### One-Time Ranger Configuration

Add whitelisting for `tez\.grouping\.*` in Hive on Tez configuration to allow HMS Mirror to pass Tez-specific properties.

### Ranger Policy Setup

Add the user executing HMS Mirror (e.g., "hive") to the Deny rule in Ranger for policy "All Volume, Bucket only" to prevent unwanted automatic volume creation.

## Ozone Volume and Bucket Creation

### Script: CreateOzoneVolumesAndBuckets.py

This script creates Ozone volumes and buckets corresponding to HDFS FID directories.

### Configuration File

Create `createScriptConf.txt` with the following parameters:

```ini
fid_dir_prefix=/data/
volumeOwner=hdfs
run_directory=.
keytab_path=/root/hdfs.keytab
principal=hdfs/hostname@REALM

default_volume_quota=12GB
buckets=raw|managed|work
encrypted=N
replication_type=RATIS
layout=FILE_SYSTEM_OPTIMIZED
```

### Default Run

Scans all directories under `/data` and creates corresponding volumes and buckets:

```bash
# Dry run
./CreateOzoneVolumesAndBuckets.py -f createScriptConf.txt -r

# Execute
./CreateOzoneVolumesAndBuckets.py -f createScriptConf.txt
```

### Custom Run with Parameters

Create `customCreateInput.txt`:

```
FID1, 1gb, raw|managed|work, N, RATIS, FILE_SYSTEM_OPTIMIZED
FID2, 2gb, raw|managed|work, N, RATIS, FILE_SYSTEM_OPTIMIZED
FID3, 3gb, raw|managed|work, N, RATIS, FILE_SYSTEM_OPTIMIZED
```

Execute:

```bash
# Dry run
./CreateOzoneVolumesAndBuckets.py -f createScriptConf.txt -c customCreateInput.txt -r

# Execute
./CreateOzoneVolumesAndBuckets.py -f createScriptConf.txt -c customCreateInput.txt
```

### Output

The script generates:
- Log file: `HDFS2OzoneNamespace.log`
- Shell script: `ozoneCreateScript.sh`

## HMS Mirror Automation

### Configuration File: input_file.conf

Create an input configuration file with your environment settings:

```ini
host=<MySQL_HOST>
user=<Username>
password=<Password>
connect_db=<Hive_Metastore_DB>
query_db="<comma_separated_db_names>"
create_table_exclude_list="true"
DB_NAME="<comma_separated_db_names>"
OZ_NAME="<OFS_Nameservice>"
EWD_NAME="<pipe_separated_external_warehouse_dirs>"
MAN_NAME="<Managed_Warehouse_dir>"
HDFS_EWD_NAME="<pipe_separated_hdfs_external_warehouse_dirs>"
OZONE_PREFIX="ozone_"
```

### Example Configuration

```ini
host=mysql-hostname
user=hive_user
password=SecurePassword123
connect_db=hive1

query_db="hdfs_db4,hdfs_db5"
create_table_exclude_list="true"
DB_NAME="hdfs_db4,hdfs_db5"
OZ_NAME="ofs://ozone1756774157"
EWD_NAME="/fid2/raw/hive|/fid2/managed/hive"
MAN_NAME="/warehouse/tablespace/managed/hive"
HDFS_EWD_NAME="/data/fid2/raw/hive|/data/fid2/managed/hive"
OZONE_PREFIX="ozone_"
```

### Step 1: Get Table Exclusion List

Identifies tables with locations outside their database directory:

```bash
python get_table_exclusion_list.py -i input_file.conf
```

**Behavior:**
- If `create_table_exclude_list="true"`: Adds `TABLES_EXCLUDE_LIST` to `input_file.conf`
- If `create_table_exclude_list="false"`: Outputs to `./reports/run_get_table_exclusion_list_<timestamp>/create_table_exclude_filter_list_output.txt`

### Step 2: Execute HMS Mirror Automation

#### Validate HMS Mirror Commands

```bash
python3 execute_hms_mirror.py -c input_file.conf --generate-scripts --validate-hms-mirror-cmds
```

#### Generate Migration Scripts

```bash
python3 execute_hms_mirror.py -c input_file.conf --generate-scripts
```

This tool:
- Generates optimized HMS Mirror commands
- Executes migrations for specified databases
- Post-processes generated SQL files for clean deployment

## Usage

### Complete Migration Workflow

1. **Setup HMS Mirror**
   ```bash
   export JAVA_HOME=/opt/jdk-17
   export PATH=$JAVA_HOME/bin:$PATH
   hms-mirror --setup
   ```

2. **Start HMS Mirror Service**
   ```bash
   hms-mirror --service --server.port=8090
   ```

3. **Create Ozone Volumes and Buckets**
   ```bash
   ./CreateOzoneVolumesAndBuckets.py -f createScriptConf.txt
   ```

4. **Authenticate (if using Kerberos)**
   ```bash
   kinit -kt /path/to/hive.keytab <principal>
   ```

5. **Generate Table Exclusion List**
   ```bash
   python get_table_exclusion_list.py -i input_file.conf
   ```

6. **Validate and Execute Migration**
   ```bash
   python3 execute_hms_mirror.py -c input_file.conf --generate-scripts --validate-hms-mirror-cmds
   python3 execute_hms_mirror.py -c input_file.conf --generate-scripts
   ```

## Configuration

### HMS Mirror YAML Configuration

Key sections to update:

```yaml
clusters:
  left:
    hcfsNamespace: "hdfs://ns1"
    hiveServer2:
      uri: "jdbc:hive2://..."
      jarFile: "/path/to/jars"
    metastore_direct:
      uri: "jdbc:mysql://..."
      
  right:
    hcfsNamespace: "ofs://ozone-service"
    hiveServer2:
      uri: "jdbc:hive2://..."
```

### Migration Strategies

Two YAML configurations are used:

1. **Storage Migration**: Full data and metadata migration
2. **Schema Only**: Metadata-only migration

## Known Limitations

- **External Tables Only**: This tool works with external Hive tables
- **Iceberg Tables**: Not supported; requires separate migration tool
- **Table Locations**: Tables with locations outside their database directory must be excluded
- **Kerberos**: Both clusters must trust the same Kerberos ticket (if applicable)

## Troubleshooting

### Common Issues

1. **pip command not found**
   ```bash
   python -m pip install requests
   ```

2. **Java version mismatch**
   ```bash
   export JAVA_HOME=/opt/jdk-17
   export PATH=$JAVA_HOME/bin:$PATH
   java -version
   ```

3. **Kerberos ticket expired**
   ```bash
   kinit -kt /path/to/keytab <principal>
   klist  # Verify ticket
   ```

4. **MySQL connection issues**
   - Verify MySQL host, port, username, and password
   - Ensure network connectivity
   - Check metastore database name

### Logs and Reports

- HMS Mirror logs: Check execution directory
- Table exclusion reports: `./reports/run_get_table_exclusion_list_<timestamp>/`
- Ozone creation logs: `HDFS2OzoneNamespace.log`

## Support and Resources


