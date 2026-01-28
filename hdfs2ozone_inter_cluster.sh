#!/usr/bin/env bash
#############################################################################
# Cross-Cluster DistCp: HDFS -> Remote Ozone (HA via service ID, run as hive)
# MODIFIED: Dynamically extracts base path from distcp source file
#############################################################################

set -e
set -u

# Per-UID ticket cache
export KRB5CCNAME="FILE:/tmp/krb5cc_$(id -u)"

#############################################################################
# LOAD CONFIG FROM ARGUMENT FILE
#############################################################################

if [ $# -lt 1 ]; then
  echo "Usage: $0 <input_param.conf>"
  exit 1
fi

CONFIG_FILE="$1"

if [ ! -f "${CONFIG_FILE}" ]; then
  echo "ERROR: Config file not found: ${CONFIG_FILE}"
  exit 1
fi

echo "Using config file: ${CONFIG_FILE}"

# shellcheck source=/dev/null
. "${CONFIG_FILE}"

# Basic sanity checks for required variables
: "${TARGET_OZONE_SERVICE:?TARGET_OZONE_SERVICE must be set in config file}"
: "${OM92_ID:?OM92_ID must be set in config file}"
: "${OM94_ID:?OM94_ID must be set in config file}"
: "${OM93_ID:?OM93_ID must be set in config file}"
: "${OM92_HOST:?OM92_HOST must be set in config file}"
: "${OM94_HOST:?OM94_HOST must be set in config file}"
: "${OM93_HOST:?OM93_HOST must be set in config file}"
: "${OZONE_OM_PORT:?OZONE_OM_PORT must be set in config file}"
: "${SOURCE_DISTCP_FILE:?SOURCE_DISTCP_FILE must be set in config file}"
: "${OZONE_JARS_PATH:?OZONE_JARS_PATH must be set in config file}"
: "${CUSTOM_CONF_DIR:?CUSTOM_CONF_DIR must be set in config file}"
: "${KERBEROS_KEYTAB:?KERBEROS_KEYTAB must be set in config file}"
: "${KERBEROS_PRINCIPAL:?KERBEROS_PRINCIPAL must be set in config file}"
: "${DISTCP_BANDWIDTH_MB:?DISTCP_BANDWIDTH_MB must be set in config file}"
: "${DISTCP_NUM_MAPS:?DISTCP_NUM_MAPS must be set in config file}"
: "${DISTCP_MEMORY_MB:?DISTCP_MEMORY_MB must be set in config file}"

# Derive OM node list for HA
OZONE_OM_NODES="${OM92_ID},${OM94_ID},${OM93_ID}"

#############################################################################
# PREP WORK
#############################################################################

echo "=========================================="
echo "Cross-Cluster DistCp: HDFS -> Remote Ozone (HA via service ID, run as hive)"
echo "Remote OM Service ID : ${TARGET_OZONE_SERVICE}"
echo "Remote OM Nodes      : ${OZONE_OM_NODES}"
echo "=========================================="

#############################################################################
# VALIDATE OZONE CLIENT JARS
#############################################################################
echo "Step 1: Validating Ozone client jars..."

if [ ! -d "$OZONE_JARS_PATH" ]; then
  echo "ERROR: Missing Ozone JAR directory: $OZONE_JARS_PATH"
  exit 1
fi

OZONE_CLIENT_JAR=$(ls -1 "${OZONE_JARS_PATH}"/ozone-client-*.jar 2>/dev/null | head -1)
OZONE_COMMON_JAR=$(ls -1 "${OZONE_JARS_PATH}"/ozone-common-*.jar 2>/dev/null | head -1)
HDDS_COMMON_JAR=$(ls -1 "${OZONE_JARS_PATH}"/hdds-common-*.jar 2>/dev/null | head -1)
HDDS_HADOOP_JAR=$(ls -1 "${OZONE_JARS_PATH}"/hdds-hadoop-dependency-client-*.jar 2>/dev/null | head -1)

if [ -z "$OZONE_CLIENT_JAR" ]; then
    echo "ERROR: Missing ozone-client jar."
    exit 1
fi

echo "✓ Ozone jars found:"
echo "  - $(basename "$OZONE_CLIENT_JAR")"
[ -n "$OZONE_COMMON_JAR" ] && echo "  - $(basename "$OZONE_COMMON_JAR")"
[ -n "$HDDS_COMMON_JAR" ] && echo "  - $(basename "$HDDS_COMMON_JAR")"
[ -n "$HDDS_HADOOP_JAR" ] && echo "  - $(basename "$HDDS_HADOOP_JAR")"
echo ""

export HADOOP_CLASSPATH="${OZONE_CLIENT_JAR}:${OZONE_COMMON_JAR}:${HDDS_COMMON_JAR}:${HDDS_HADOOP_JAR}:${HADOOP_CLASSPATH:-}"

#############################################################################
# CUSTOM CONF DIRECTORY + REMOTE ozone-site.xml
#############################################################################
echo "Step 2: Preparing custom conf directory for REMOTE Ozone..."

if [ ! -d "$CUSTOM_CONF_DIR" ]; then
    mkdir -p "$CUSTOM_CONF_DIR"
fi

# Start by copying local Hadoop configs to the custom dir
cp /etc/hadoop/conf/* "$CUSTOM_CONF_DIR/" 2>/dev/null || true

# Overwrite/create ozone-site.xml for REMOTE Ozone cluster
OZONE_SITE_XML="${CUSTOM_CONF_DIR}/ozone-site.xml"
cat > "$OZONE_SITE_XML" << EOF
<?xml version="1.0"?>
<configuration>
  <!-- Remote Ozone HA service configuration -->
  <property>
    <name>ozone.service.id</name>
    <value>${TARGET_OZONE_SERVICE}</value>
  </property>

  <property>
    <name>ozone.om.address.${TARGET_OZONE_SERVICE}.${OM92_ID}</name>
    <value>${OM92_HOST}:${OZONE_OM_PORT}</value>
  </property>

  <property>
    <name>ozone.om.address.${TARGET_OZONE_SERVICE}.${OM94_ID}</name>
    <value>${OM94_HOST}:${OZONE_OM_PORT}</value>
  </property>

  <property>
    <name>ozone.om.address.${TARGET_OZONE_SERVICE}.${OM93_ID}</name>
    <value>${OM93_HOST}:${OZONE_OM_PORT}</value>
  </property>

  <property>
    <name>ozone.om.service.ids</name>
    <value>${TARGET_OZONE_SERVICE}</value>
  </property>

  <property>
    <name>ozone.om.nodes.${TARGET_OZONE_SERVICE}</name>
    <value>${OZONE_OM_NODES}</value>
  </property>

  <!-- OFS filesystem binding -->
  <property>
    <name>fs.ofs.impl</name>
    <value>org.apache.hadoop.fs.ozone.OzoneFileSystem</value>
  </property>

  <property>
    <name>fs.AbstractFileSystem.ofs.impl</name>
    <value>org.apache.hadoop.fs.ozone.OzFs</value>
  </property>

  <!-- Security / Kerberos -->
  <property>
    <name>ozone.security.enabled</name>
    <value>true</value>
  </property>

<!--  <property>
    <name>ozone.om.kerberos.principal</name>
    <value>om/_HOST@VPC.CLOUDERA.COM</value>
  </property> -->

  <!-- Failover and retry settings -->
  <property>
    <name>ozone.client.failover.max.attempts</name>
    <value>15</value>
  </property>

  <property>
    <name>ozone.client.connection.timeout</name>
    <value>30000</value>
  </property>
</configuration>
EOF

echo "✓ Custom REMOTE Ozone config created at $OZONE_SITE_XML"
echo ""

# Use ONLY our custom conf dir
export HADOOP_CONF_DIR="$CUSTOM_CONF_DIR"

#############################################################################
# DISCOVER REMOTE OM LEADER AND BOOTSTRAP ADDRESS
#############################################################################

# Use the same service ID for admin roles query
OZONE_ADMIN_SERVICE_ID="${TARGET_OZONE_SERVICE}"

echo "Step 3: Discovering remote OM leader via 'ozone admin om roles -id ${OZONE_ADMIN_SERVICE_ID}'..."

LEADER_LINE="$(ozone admin om roles -id "${OZONE_ADMIN_SERVICE_ID}" 2>/dev/null | awk '/ LEADER /')"

LEADER_OM_ADDR=""
LEADER_HOST=""

if [ -n "${LEADER_LINE}" ]; then
  echo "Raw leader line: ${LEADER_LINE}"

  # Example:
  # om94 : LEADER (vvs-baseiq-4.vpc.cloudera.com)
  # Extract hostname inside parentheses
  LEADER_HOST="$(echo "${LEADER_LINE}" | awk -F'[()]' '{print $2}')"

  if [ -n "${LEADER_HOST}" ]; then
    LEADER_OM_ADDR="${LEADER_HOST}:${OZONE_OM_PORT}"
    echo "✓ Discovered remote OM leader host: ${LEADER_HOST}"
    echo "✓ Remote OM leader address    : ${LEADER_OM_ADDR}"
  else
    echo "WARN: Could not parse leader host from LEADER_LINE, falling back to ozone-site.xml"
  fi
else
  echo "WARN: No LEADER line found from 'ozone admin om roles'; falling back to ozone-site.xml"
fi

# Fallback: pick first OM address from ozone-site.xml if leader detection failed
if [ -z "${LEADER_OM_ADDR}" ]; then
  LEADER_OM_ADDR=$(
    awk -v sid="$TARGET_OZONE_SERVICE" '
      /<name>ozone\.om\.address\./ && index($0, sid) {
        getline
        if ($0 ~ /<value>/) {
          split($0, a, /[<>]/)
          print a[3]
          exit
        }
      }
    ' "$OZONE_SITE_XML"
  )
  echo "Fallback remote OM leader address from ozone-site.xml: ${LEADER_OM_ADDR}"
fi

if [ -z "${LEADER_OM_ADDR}" ]; then
  echo "ERROR: Could not determine remote OM leader address (leader or fallback)."
  exit 1
fi

#############################################################################
# EXTRACT BASE PATH FROM DISTCP SOURCE FILE
#############################################################################

echo "Step 3a: Extracting base path from DistCp source file..."

# Check if distcp source file exists
if [ ! -f "$SOURCE_DISTCP_FILE" ]; then
  echo "ERROR: DistCp source file not found: $SOURCE_DISTCP_FILE"
  exit 1
fi

# Read first line from the local distcp source file
FIRST_LINE=$(head -1 "$SOURCE_DISTCP_FILE" 2>/dev/null)

if [ -z "$FIRST_LINE" ]; then
  echo "ERROR: Could not read first line from $SOURCE_DISTCP_FILE"
  exit 1
fi

echo "First source path: $FIRST_LINE"

# Extract path between "hdfs://<nameservice>/data/" and last "/"
# Example: hdfs://ns1/data/fid2/raw/hive/hdfs_db4/csvtable1
# Result: fid2/raw/hive/hdfs_db4

# Use sed to remove the hdfs://<nameservice>/data/ prefix (any nameservice)
TEMP_PATH=$(echo "$FIRST_LINE" | sed 's|^hdfs://[^/]*/data/||')

# Remove everything after the last "/"
OZONE_BASE_PATH="${TEMP_PATH%/*}"

if [ -z "$OZONE_BASE_PATH" ]; then
  echo "ERROR: Could not extract base path from: $FIRST_LINE"
  exit 1
fi

echo "✓ Extracted base path: $OZONE_BASE_PATH"
echo ""

TARGET_OZONE_PATH="ofs://${LEADER_OM_ADDR}/${OZONE_BASE_PATH}"
echo "Target Ozone path: ${TARGET_OZONE_PATH}"
echo ""

#############################################################################
# KERBEROS
#############################################################################

if [ "${KERBEROS_ENABLED}" == "true" ]; then
  echo "Step 4: Authenticating with Kerberos as hive..."

  if [ ! -f "$KERBEROS_KEYTAB" ]; then
     echo "ERROR: hive keytab not found at $KERBEROS_KEYTAB"
     exit 1
  fi

  echo "Using keytab: $KERBEROS_KEYTAB"
  echo "Using principal: $KERBEROS_PRINCIPAL"
  echo ""

  kinit -kt "$KERBEROS_KEYTAB" "$KERBEROS_PRINCIPAL"

  if [ $? -eq 0 ]; then
    echo "✓ Kerberos login complete"
    echo ""
    klist | head -5
    echo ""
  else
    echo "ERROR: Kerberos authentication failed"
    exit 1
  fi
fi

#############################################################################
# TEST CONNECTIVITY
#############################################################################

echo "Step 5: Test HDFS access as hive"
if ! hdfs dfs -ls / &>/dev/null; then
  echo "ERROR: HDFS not accessible as hive"
  exit 1
fi
echo "✓ HDFS OK"
echo ""

echo "Step 6: Test remote Ozone access via OM leader ${LEADER_OM_ADDR}"
echo "Command: hadoop fs -ls ofs://${LEADER_OM_ADDR}/"
if hadoop fs -ls "ofs://${LEADER_OM_ADDR}/" &>/dev/null; then
  echo "✓ Remote Ozone reachable via $LEADER_OM_ADDR"
else
  echo "ERROR: Cannot reach remote Ozone using $LEADER_OM_ADDR"
  echo ""
  echo "Attempting to show actual error:"
  hadoop fs -ls "ofs://${LEADER_OM_ADDR}/" 2>&1 | head -20
  exit 1
fi
echo ""

#############################################################################
# PREPARE DISTCP FILE
#############################################################################

echo "Step 7: Preparing DistCp source file..."

if [ -z "${HCFS_BASE_DIR+x}" ]; then
   HCFS_BASE_DIR="/tmp/distcp"
fi

echo "Using HCFS_BASE_DIR: $HCFS_BASE_DIR"
hdfs dfs -mkdir -p "$HCFS_BASE_DIR"

if [ ! -f "$SOURCE_DISTCP_FILE" ]; then
  echo "ERROR: DistCp source file not found: $SOURCE_DISTCP_FILE"
  echo ""
  echo "Make sure you copied it, e.g.:"
  echo "  cp hdfs_db4_LEFT_1_distcp_source.txt /tmp/"
  echo "  chown hive:hive /tmp/hdfs_db4_LEFT_1_distcp_source.txt"
  exit 1
fi

SRC_BASENAME="$(basename "$SOURCE_DISTCP_FILE")"

hdfs dfs -copyFromLocal -f "$SOURCE_DISTCP_FILE" "${HCFS_BASE_DIR}/${SRC_BASENAME}"
echo "✓ DistCp list copied to HDFS: ${HCFS_BASE_DIR}/${SRC_BASENAME}"
echo ""

#############################################################################
# RUN DISTCP
#############################################################################

echo "Step 8: Running DistCp..."
echo "=========================================="
echo "Source: HDFS (this cluster)"
echo "Target: $TARGET_OZONE_PATH"
echo "Bootstrap OM: $LEADER_OM_ADDR"
echo "Bandwidth: ${DISTCP_BANDWIDTH_MB} MB/mapper"
echo "Mappers: ${DISTCP_NUM_MAPS}"
echo "Memory per mapper: ${DISTCP_MEMORY_MB} MB"
echo "=========================================="
echo ""

DISTCP_OPTS=""
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.job.queuename=default"
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.map.memory.mb=${DISTCP_MEMORY_MB}"
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.map.java.opts=-Xmx$((DISTCP_MEMORY_MB * 80 / 100))m"

# Use bootstrap host for token-renewal.exclude
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.job.hdfs-servers.token-renewal.exclude=${LEADER_HOST}"
DISTCP_OPTS="$DISTCP_OPTS -Dmapred.job.hdfs-servers.token-renewal.exclude=${LEADER_HOST}"

DISTCP_OPTS="$DISTCP_OPTS -Dyarn.resourcemanager.delegation-token.renew-interval=-1"
DISTCP_OPTS="$DISTCP_OPTS -bandwidth ${DISTCP_BANDWIDTH_MB}"
DISTCP_OPTS="$DISTCP_OPTS -m ${DISTCP_NUM_MAPS}"

echo "DistCp Command:"
echo "hadoop distcp ${DISTCP_OPTS} -skipcrccheck -log /tmp/distcp-logs-hdfs_db4 -f ${HCFS_BASE_DIR}/${SRC_BASENAME} ${TARGET_OZONE_PATH}"
echo ""

set +e
hadoop distcp ${DISTCP_OPTS} \
    -skipcrccheck \
    -log /tmp/distcp-logs-hdfs_db4 \
    -f "${HCFS_BASE_DIR}/${SRC_BASENAME}" \
    "${TARGET_OZONE_PATH}"
RET=$?
set -e

echo ""
echo "=========================================="
if [ $RET -eq 0 ]; then
  echo "  ✓ DISTCP COMPLETED SUCCESSFULLY!"
  echo ""
  echo "  Verification:"
  echo "    hadoop fs -ls ${TARGET_OZONE_PATH}"
  echo "    hadoop fs -count ${TARGET_OZONE_PATH}"
  echo ""
else
  echo "  ✗ DISTCP FAILED — EXIT CODE $RET"
  echo ""
  echo "  Troubleshooting steps:"
  echo "    1. Check YARN application logs:"
  echo "       yarn application -list"
  echo "       yarn logs -applicationId <app-id>"
  echo ""
  echo "    2. Check DistCp logs:"
  echo "       hdfs dfs -cat /tmp/distcp-logs-hdfs_db4/_logs/*"
  echo ""
  echo "    3. Check Ozone OM logs on remote cluster nodes:"
  echo "       ${OM92_HOST}, ${OM94_HOST}, ${OM93_HOST}"
  echo ""
  echo "    4. Verify Ranger/ACL permissions for user hive on path:"
  echo "       ${TARGET_OZONE_PATH}"
  echo ""
fi
echo "=========================================="

exit $RET
