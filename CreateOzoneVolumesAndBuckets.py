#! /usr/bin/python


'''
Read input file which has row entries in following format and create ozone volumes and buckets.

Row fields are - volumeName, quota, bucketNames, encryption, replicationType, layout
*volumeName - will be FID directory names under /data directory in HDFS
*quota - will be same as HDFS quota of the /data/FID directory on HDFS.
    In HDFS for eg  50g for 50 gigabytes and 2t for 2 terabytes quota
    In Ozone quota can be set at volume or bucket level. Here we are setting at volume level
*bucketNames - bucket names separated by '|' are sub directory names under /data/FID HDFS directory
    which will be for exmple raw,managed,work
*encryption - If Y, then all buckets under the volume will be encrypted.
    Default is set to N which means no encryption
*replicationType - Replication is done at bucket level.
    By default ratis replication will be done for all buckets under the volume
    replicationType = ['EC','RATIS']
    If EC then codecDataParityChunksize = ['rs-3-2-1024k', 'rs-6-3-1024k', 'rs-10-4-1024k'] # These options are only for EC
*layout - Layout is also created at bucket level. Default is ofs layout.
    Once the OFS bucket is created, you can interact with it like a traditional file system,
    creating directories, uploading files, and using paths to access subdirectories.
    layoutType = ['FILE_SYSTEM_OPTIMIZED', 'OBJECT_STORE', 'LEGACY']

Eg row will looklike - FID1, 50g, raw|managed|work, N, ratis, fso
'''



import sys
import json
import os
import subprocess
import datetime
import logging
import optparse
import re


# Initialize a global dictionary for loading input params from file
input_params = {}

# Run directory name
run_directory = ''

log_file = ''

# input config file path name
input_conf_file = ''

dry_run = False
creation_input_json_list = []
creation_params_file = ''

commands_string_list = []
ozone_create_commands_file = ""

fid_quota_and_subdirs_json_list = []

# Ozone volume bucket creation options
isEncrypted = ['Y','N']
encryptionKey = ""  #This option is only for isEncrypted = 'Y'
replicationType = ['EC','RATIS']
replicationCount = ['1', '3'] # This options are only for RATIS
layoutType = ['FILE_SYSTEM_OPTIMIZED', 'OBJECT_STORE', 'LEGACY']
codecDataParityChunksize = ['rs-3-2-1024k', 'rs-6-3-1024k', 'rs-10-4-1024k'] # These options are only for EC

error_log_file = ""

fids_file = ''
fids_list = []

custom_run = False








def is_valid_ozone_volume_name(name):
    # Check length constraint
    if not (3 <= len(name) <= 63):
        return False

    # Regex to match allowed characters (lowercase letters, digits, period and hyphens)
    if not re.match('^[a-z0-9.-]+$', name):
        return False

    # Check if name starts and ends with a lowercase letter or digit
    if not (name[0].isalnum() and name[-1].isalnum()):
        return False

    # Check for consecutive hyphens
    if '--' in name or '..' in name:
        return False

        # Check for period adjacent to hyphen (e.g., `-.` or `.-`)
    if '-.' in name or '.-' in name:
        return False


    return True








def is_valid_ozone_bucket_name(name):
    # Check length constraint
    if not (3 <= len(name) <= 63):
        return False

    # Regex to match allowed characters (lowercase letters, digits, period and hyphens)
    if not re.match('^[a-z0-9.-]+$', name):
        return False

    # Check if name starts and ends with a lowercase letter or digit
    if not (name[0].isalnum() and name[-1].isalnum()):
        return False

    # Check for consecutive hyphens
    if '--' in name or '..' in name:
        return False

        # Check for period adjacent to hyphen (e.g., `-.` or `.-`)
    if '-.' in name or '.-' in name:
        return False


    return True







def filter_invalid_volume_names():
    global fids_list

    # Use filter to apply the custom function and create a filtered list
    valid_fid_names_list = filter(is_valid_ozone_volume_name, fids_list)

    invalid_fid_names_list = [item for item in fids_list if item not in valid_fid_names_list]

    if invalid_fid_names_list:
        print "These are invalid volume names. Not creating volumes for these : " + ", ".join(invalid_fid_names_list)

    return valid_fid_names_list, invalid_fid_names_list







def filter_invalid_bucket_names(bucket_names_list):

    # Use filter to apply the custom function and create a filtered list
    valid_bucket_names_list = filter(is_valid_ozone_bucket_name, bucket_names_list)

    invalid_bucket_names_list = [item for item in bucket_names_list if item not in valid_bucket_names_list]

    if invalid_bucket_names_list:
        print "These are invalid bucket names. And will not be creating buckets for these :"
        # Print formatted output
        print ", ".join(invalid_bucket_names_list)

    return valid_bucket_names_list








def get_all_fids_from_hdfs():
    global fids_list
    global input_params
    hdfs_dirs = ''
    kinit(input_params['keytab_path'], input_params['principal'])

    command = "hdfs dfs -ls /data | grep '^d' | awk '{print $8}'"

    # Use subprocess to call the hdfs getfacl command
    try:
        # Run the command and capture the output

        output = subprocess.check_output(command, shell=True)

        # Convert the output to a list of subdirectory names (split by newline)
        subdirectories = output.strip().split('\n')
        # Collect the second string (element) from each path
        fids_list = [s.split('/')[2] for s in subdirectories if len(s.split('/')) > 1]

    except subprocess.CalledProcessError as e:
        print "Failed to get sub dirs for HDFS path: /data"








def build_creation_input_json_list():
    global creation_input_json_list
    global input_params
    global fids_list

    for fid in fids_list:
        # Create an empty dictionary
        creation_input_json = {}

        creation_input_json['volumeName'] = fid

        creation_input_json['volumeOwner'] = input_params['volumeOwner']

        creation_input_json['volumeSpaceQuota'] = input_params['default_volume_quota']

        bucket_names_string = input_params['buckets']
        bucket_names_list = bucket_names_string.split('|')

        valid_bucket_names_list = filter_invalid_bucket_names(bucket_names_list)

        creation_input_json['bucketNames'] = valid_bucket_names_list

        creation_input_json['isEncrypted'] = input_params['encrypted']

        creation_input_json['replicationType'] = input_params['replication_type']

        creation_input_json['layoutType'] = input_params['layout']

        # Add the dictionary to the list
        creation_input_json_list.append(creation_input_json)

    return








def convert_bytes_to_mb_gb_tb(bytes_value):

    kb_value = float(bytes_value) / (1024)
    mb_value = float(bytes_value) / (1024 ** 2)
    gb_value = float(bytes_value) / (1024 ** 3)
    tb_value = float(bytes_value) / (1024 ** 4)


    if kb_value < 1024:
        return str(int(kb_value)) + "KB"
    elif mb_value < 1024:
        return str(int(mb_value)) + "MB"
    elif gb_value < 1024:
        return str(int(gb_value)) + "GB"
    elif tb_value < 1024:
        return str(int(tb_value)) + "TB"




def get_all_fids_from_creation_input_json_list():
    global creation_input_json_list
    global fids_list

    for item in creation_input_json_list:
        fids_list.append(item['volumeName'])







def load_creation_input_file(creation_params_file):
    global creation_input_json_list
    global isEncrypted
    global replicationType
    global layoutType
    global input_params

    if os.path.exists(creation_params_file):
        with open(creation_params_file, 'r') as file:
            # Iterate through each line in the file
            for line in file:
                # Strip any leading/trailing whitespace (like newlines)
                line = line.strip()
                if not line:  # Check if the line is empty
                    continue
                #print (line, "\n")
                # Split the line into values list
                values = line.split(',')


                # Length of values list shoule be equal to 6
                if len(values) < 6:
                    print "Invalid input line. Proceeding to next one : ", "(" ,line, ")"
                    #if values count is not eqaul to 6 then continue to next line
                    continue
                else:
                    # Add values to disctonay
                    # Create an empty dictionary
                    creation_input_json = {}
                    # Assign 6 values to 6 keys
                    creation_input_json['volumeName'] = values[0].strip()
                    # Convert unit like tb,gb,mb to upper case
                    creation_input_json['volumeSpaceQuota'] = values[1].strip().upper()

                    # Split the bucket names string on the '|' delimiter
                    bucket_names_list = values[2].strip().split('|')

                    valid_bucket_names_list = filter_invalid_bucket_names(bucket_names_list)

                    creation_input_json['bucketNames'] = valid_bucket_names_list

                    creation_input_json['isEncrypted'] = values[3].strip().upper()
                    if creation_input_json['isEncrypted'] not in isEncrypted:
                        continue
                    creation_input_json['replicationType'] = values[4].strip().upper()
                    if creation_input_json['replicationType'] not in replicationType:
                        continue
                    creation_input_json['layoutType'] = values[5].strip().upper()
                    if  creation_input_json['layoutType'] not in layoutType:
                        continue
                    #print creation_input_json, "\n\n"

                    creation_input_json['volumeOwner'] = input_params['volumeOwner']

                    # Add the dictionary to the list
                    creation_input_json_list.append(creation_input_json)

    else:
        print "Ozone creation params input file does not exist : " + creation_params_file
        sys.exit(1)








def load_fids_from_input_file(fid_input_file):
    global fids_list

    # Open the file in read mode
    if os.path.exists(fid_input_file):
        with open(fid_input_file, "r") as file:
            for line in file:

                # Strip any leading/trailing whitespace (like newlines)
                line = line.strip()

                if not line:  # Check if the line is empty
                    continue

                # split line by commas
                elements = line.split(',')
                # Strip spaces from each element
                elements = [element.strip() for element in elements]
                # Append all elements of list2 to list1
                fids_list += elements

    else:
        print "Fid file does not exist : " + fid_input_file
        sys.exit(1)






# Retrieve a list of Ozone volumes
def get_existing_ozone_volumes():
    global input_params
    volume_json_list = []

    kinit(input_params['keytab_path'], input_params['principal'])

    try:
        # Run the command to list volumes
        result_text = subprocess.check_output(['ozone', 'sh', 'volume', 'list'], stderr=subprocess.STDOUT)
        # Convert plain text to json
        volume_json_list = json.loads(result_text)

    except subprocess.CalledProcessError as e:
        print("Error retrieving volumes: {}".format(e.output))
        sys.exit(1)


    volume_name_list = []

    # Loop through list and get volume names
    for item in volume_json_list:
        volume_name_list.append(item['name'])

    return volume_name_list







# Retrieve quota on in existing volume
def get_remaining_volume_quota_bytes(volume_name):
    global input_params

    volume_info_json = {}
    bucket_json_list = []

    volumequotaInBytes = 0
    totalBucketquotaInBytes = 0
    remainingVolumequotaInBytes = 0


    kinit(input_params['keytab_path'], input_params['principal'])


    try:
        # Run the command to list buckets in the volume
        result_text1 = subprocess.check_output(['ozone', 'sh', 'bucket', 'list', volume_name], stderr=subprocess.STDOUT)
        # Convert plain text to json
        bucket_json_list = json.loads(result_text1)

        result_text2 = subprocess.check_output(['ozone', 'sh', 'volume', 'info', volume_name], stderr=subprocess.STDOUT)
        volume_info_json = json.loads(result_text2)

    except subprocess.CalledProcessError as e:
        print("Error retrieving volume list/info: {}".format(e.output))
        sys.exit(1)


    volumequotaInBytes = int(volume_info_json['quotaInBytes'])

    # Loop through list and sum all the bucket quotas
    for item in bucket_json_list:
        totalBucketquotaInBytes += int(item['quotaInBytes'])

    remainingVolumequotaInBytes = volumequotaInBytes - totalBucketquotaInBytes


    return remainingVolumequotaInBytes







# Retrieve a list of buckets in the specified Ozone volume
def get_existing_buckets_in_volume(volume_name):
    global input_params
    bucket_json_list = []

    kinit(input_params['keytab_path'], input_params['principal'])
    try:
        # Run the command to list buckets in the volume
        result_text = subprocess.check_output(['ozone', 'sh', 'bucket', 'list', volume_name], stderr=subprocess.STDOUT)
        # Convert plain text to json
        bucket_json_list = json.loads(result_text)

    except subprocess.CalledProcessError as e:
        print("Error retrieving buckets: {}".format(e.output))
        sys.exit(1)


    bucket_names_list = []

    # Loop through list and get volume names
    for item in bucket_json_list:
        bucket_names_list.append(item['name'])

    return bucket_names_list







def validate_quota_string(quota_str):

    valid_quota = True
    unit_str_list = ['MB', 'GB', 'TB']
    # Use a regular expression to extract the numeric and string parts
    match = re.match(r"(\d+)\s*([a-zA-Z]+)", quota_str)

    if match:
        size = match.group(1)  # Extract the numeric part
        unit = match.group(2)   # Extract the string part

    size_number = int(size)
    unit_str = unit.upper()


    if unit_str not in unit_str_list:
        print "Invalid Volume quota :", quota_str
        valid_quota = False


    return valid_quota






def convert_quota_str_to_bytes(volume_quota_str):

    quotaInBytes = 0
    # Use a regular expression to extract the numeric and string parts
    match = re.match(r"(\d+)\s*([a-zA-Z]+)", volume_quota_str)
    size = ""
    unit = ""


    if match :
        size = match.group(1)  # Extract the numeric part
        unit = match.group(2)   # Extract the string part
    else:
        print "Invalid Volume quota: ", volume_quota_str
        print "Should be a positve whole number like 10GB "
        return quotaInBytes


    size_number = int(size)


    if unit.upper() == 'KB':
        quotaInBytes = size_number*1024
    if unit.upper() == 'MB':
        quotaInBytes = size_number*1024*1024
    if unit.upper() == 'GB':
        quotaInBytes = size_number*1024*1024*1024
    if unit.upper() == 'TB':
        quotaInBytes = size_number*1024*1024*1024*1024

    return quotaInBytes






def compute_bucket_quota_from_volume_quota(volume_quota_str, number_of_buckets):
    bucket_quota_str =''
    quotaInBytes = 0
    quotaInBytes = convert_quota_str_to_bytes(volume_quota_str)

    if quotaInBytes < 1024:
        return bucket_quota_str


    bucket_quota_num = quotaInBytes/number_of_buckets
    if bucket_quota_num == 0:
        return bucket_quota_str

    bucket_quota_str = convert_bytes_to_mb_gb_tb(bucket_quota_num)
    #print "Bucket quota:", bucket_quota_str

    return bucket_quota_str







def buildOzoneCommands():

    global commands_string_list
    global creation_input_json_list
    global fids_list

    # Get list of valid and invalid volume names from the input fid list provided withe rin custom, or full run on fustomcreation inputs
    valid_fids_list, invalid_fids_list = filter_invalid_volume_names()

    # Get list of existing ozone volumes
    existing_ozone_volumes = get_existing_ozone_volumes()

    print "Starting building ozone create commands"
    print "-----------------------------------------"

    for creation_input_json in creation_input_json_list:

        # If volume name is in invalid format, then skip and move on to next volume creation
        if creation_input_json['volumeName'] in invalid_fids_list:
            continue
        # Initialize empty bucket list for every volume
        existing_buckets_in_volume = []
        remaining_volume_quota_bytes = 0
        create_volume_command = ''
        bucket_space_quota_str = ''

        # Create Volume only if it doesn't exist
        if creation_input_json['volumeName'] and creation_input_json['volumeName'] not in existing_ozone_volumes:
            # Build create volume ozone command like "ozone sh volume create /myvolume --quota 10GB --user myuserid"
            #create_volume_command = "\n\nozone sh volume create /" + creation_input['volumeName'] + " --quota " + \
            #                creation_input['quota'] + " --user " + input_params['volumeOwner']
            if not validate_quota_string(creation_input_json['volumeSpaceQuota']):
                print "Continuing to next voulume/bucket creation"
                continue
            create_volume_command = "ozone sh volume create /" + creation_input_json['volumeName'] + " --quota " + \
                                    creation_input_json['volumeSpaceQuota'] + " --user " + creation_input_json['volumeOwner']

            # Compute new buckets quota from given volume quota
            bucket_space_quota_str = compute_bucket_quota_from_volume_quota(creation_input_json['volumeSpaceQuota'], len(creation_input_json['bucketNames']))
            commands_string_list.append(create_volume_command)
            print create_volume_command

        else: # If volume exists, compute remianing volume quota and existingbuckets list
            print "Volume already exists, proceeding to create buckets for this volume : " + creation_input_json['volumeName']
            remaining_volume_quota_bytes = get_remaining_volume_quota_bytes(creation_input_json['volumeName'])
            if remaining_volume_quota_bytes < 1024 : # if remaining volume quota is less than 1MB then cotinue without creating buckets
                print "Low remaining volume quota in volume. Cannot create new buckets : " + creation_input_json['volumeName']
                print "Continuing to next voulume/bucket creation"
                continue
            existing_buckets_in_volume = get_existing_buckets_in_volume(creation_input_json['volumeName'])
            remaining_volume_quota_str = convert_bytes_to_mb_gb_tb(remaining_volume_quota_bytes)

            # Compute new buckets quota fromm remaing volume quota
            bucket_space_quota_str = compute_bucket_quota_from_volume_quota(remaining_volume_quota_str, len(creation_input_json['bucketNames']))

        # Loop through bucket name list in a given volume and construct bucket create commands
        for bucket in creation_input_json['bucketNames']:
            create_bucket_command = ''

            # Create new bucket only if it does not already exist
            if  bucket not in existing_buckets_in_volume:
                if bucket_space_quota_str == '':
                    print "Low/zero bucket quota. Continuing to next volume"
                    continue
                create_bucket_command = "ozone sh bucket create --space-quota " + bucket_space_quota_str + \
                                        " /" + creation_input_json['volumeName']  + "/" + bucket + \
                " --replication-type " + creation_input_json['replicationType'] + " --replication 3 --layout " + \
                                        creation_input_json['layoutType'] + " --user " + creation_input_json['volumeOwner']


                commands_string_list.append(create_bucket_command)
                print "\n", create_bucket_command
            else:
                print "Bucket already exists, proceeding to create next bucket/volume : " + bucket









def writeOzoneCommandsFile():
    global commands_string_list
    global ozone_create_commands_file

    # Open a file in write mode
    with open(ozone_create_commands_file, 'w') as file:
        # Iterate over the list and write each fruit to a new line
        for item in commands_string_list:
            file.write(item + '\n')

    print "Generated ozone script for volume and bucket creation: " + ozone_create_commands_file





def executeOzoneCommandsFile():
    global ozone_create_commands_file
    global input_params
    global log_file
    global commands_string_list
    global error_log_file

    if len(commands_string_list) == 0:
        return

    # Set the permissions to make the script executable (owner can read/write/execute)
    permissions = 0o755  # Owner can read, write, and execute; group and others can read and execute

    # Change the file permissions
    try:
        os.chmod(ozone_create_commands_file, permissions)
        #print("Permissions for '{}' changed successfully to executable.".format(script_path))
    except Exception as e:
        print("Error changing permissions for '{}': {}".format(ozone_create_commands_file, e))
        sys.exit(1)

    kinit(input_params['keytab_path'], input_params['principal'])

    print "Ozone create script execution starting: " + ozone_create_commands_file
    print "----------------------------------------------------------------------"

    try:
        # Use subprocess.Popen to capture output from the shell script
        process = subprocess.Popen(
            ["/bin/bash", ozone_create_commands_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Read output line by line to log and print in real-time
        for line in iter(process.stdout.readline, ''):
            print line.strip()
            #logger.info("CURL_COMMAND_STDOUT: %s", line.strip())
        for line in iter(process.stderr.readline, ''):
            print line.strip()
            #logger.error("CURL_COMMAND_STDERR: %s", line.strip())

        # Wait for the process to complete
        process.wait()

        # Check the return code
        if process.returncode != 0:
            logger.error("Curl script exited with return code %d", process.returncode)
        else:
            logger.info("Create script execution completed")

    except Exception as e:
        logger.exception("Failed to execute Create script: %s", str(e))

    print "-----------------------------------------------------------------------------------"






# Redirect print statements to the logging system
class PrintLogger(object):
    def __init__(self, level):
        self.level = level

    def write(self, message):
        if message.strip():  # Log only non-empty messages
            self.level(message)

    def flush(self):
        pass  # Required for compatibility with file-like objects







# Function to create a unique run directory
def create_run_directory(base_dir):
    # Generate a timestamp string
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Create a unique directory name
    run_dir = os.path.join(base_dir, 'run_' + timestamp)
    # Create the base directory if it doesn't exist
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    # Create the run directory
    os.makedirs(run_dir)
    return run_dir







# Load input parameters
def load_input_params():
    global input_params
    global input_conf_file
    # Open the file and read it line by line

    if os.path.exists(input_conf_file):
        with open(input_conf_file, 'r') as p_file:
            for line in p_file:
                # Strip whitespace characters like `\n` at the end of each line
                line = line.strip()

                # Ignore empty lines
                if not line:
                    continue

                # Split by the first '=' to get the key and value
                key, value = line.split('=', 1)

                # Strip whitespace from key and value
                key = key.strip()
                value = value.strip()

                # Optionally, convert value to an appropriate type (e.g., int for age)
                if value.isdigit():
                    value = int(value)

                # Add the key-value pair to the dictionary
                input_params[key] = value
    else:
        print "Input configurations file does not exist : " + input_conf_file
        sys.exit(1)

    # Validate if all the input config parameters are specified
    validate_input_params()








# Validate if all the input config parameters are specified
def validate_input_params():
    global input_params
    valid_input = True


    if 'fid_dir_prefix' not in input_params:
        print "\nParameter 'fid_dir_prefix' is not specified in config file"
        valid_input = False

    if 'keytab_path' not in input_params:
        print "\nParameter 'keytab_path' is not specified in config file"
        valid_input = False

    if 'principal' not in input_params:
        print "\nParameter 'principal' is not specified in config file"
        valid_input = False

    if 'volumeOwner' not in input_params:
        print "\nParameter 'volumeOwner' is not specified in config file"
        valid_input = False

    if 'run_directory' not in input_params:
        print "\nParameter 'run_directory' is not specified in config file"
        valid_input = False


    if custom_run == False:
        if 'default_volume_quota' not in input_params:
            print "\nParameter 'default_volume_quota' is not specified in config file. Set it to NA for not applicable or set 500MB or 10GB or 1TB etc"
            valid_input = False

        if 'buckets' not in input_params:
            print "\nParameter 'buckets' is not specified in config file. Set it to NA for not applicable or set 500MB or 10GB or 1TB etc"
            valid_input = False

        if 'encrypted' not in input_params:
            print "\nParameter 'default_volume_quota' is not specified in config file. Set it to NA for not applicable or set 500MB or 10GB or 1TB etc"
            valid_input = False

        if 'replication_type' not in input_params:
            print "\nParameter 'replication_type' is not specified in config file. Set it to NA for not applicable or set 500MB or 10GB or 1TB etc"
            valid_input = False

        if 'layout' not in input_params:
            print "\nParameter 'layout' is not specified in config file. Set it to NA for not applicable or set 500MB or 10GB or 1TB etc"
            valid_input = False


    if valid_input is False:
        sys.exit(1)





def kinit(keytab_path, principal):

    # Define the command as a list of arguments
    kinit_command = ['kinit', '-kt', keytab_path, principal]
    kinit_success = None

    try:
        # Execute the kinit command
        subprocess.check_output(kinit_command, stderr=subprocess.STDOUT)
        kinit_success = True

    except subprocess.CalledProcessError as e:
        print '\nkinit failed'
        print '\nReturn code:', e.returncode
        # Exit the program
        sys.exit(0)

    return kinit_success






# Mondule for parsing command line options.
def main():

    global input_conf_file
    global creation_params_file
    global dry_run
    global fids_file
    global custom_run


    # Create an OptionParser object with usage and version information
    parser = optparse.OptionParser(usage="usage: %prog -f <CONF_FILE> -i <INPUT_FILE> [OPTIONS]", version="%prog 1.0")

    # Define options
    parser.add_option("-f", "--conf", dest="conf_file", help="Provide input conf file eith keytab,principal and user(volume owner)")
    parser.add_option("-r", "--dryrun", dest="dryrun",  action="store_true", default=False, help="Run all steps exept executing volume/bucket creation script")
    parser.add_option("-c", "--custom", dest="creation_params_file", help="Provide ozone create parameters in input file")
    parser.add_option("-i", "--fids", dest="fids_file", help="Run with fid file as input")


    # Parse command-line arguments
    (options, args) = parser.parse_args()


    # Check if conf file is provided. If not exit the script with error
    if not options.conf_file:
        parser.print_help()
        # Exit the program
        sys.exit(0)
    else:
        input_conf_file = options.conf_file


    # Check if create parameters input file is provided
    if options.creation_params_file:
        creation_params_file = options.creation_params_file
        custom_run = True


    # Check if fids  file is provided
    if options.fids_file:
        fids_file = options.fids_file


    if options.dryrun:
        # Set the dry_run flag
        dry_run = True









# Main function
if __name__ == '__main__':

    # Process commandline arguments with main()
    main()

    # Load all input config parameters from input file into global dictionary object input_params = {}
    load_input_params()


    # Initialize run directory base folder
    run_directory = input_params['run_directory']

    run_directory = create_run_directory(run_directory + '/' + 'createRuns')

    # Create log file name/path
    log_file = run_directory + '/' + 'HDFS2OzoneNamespace.log'

    # Create error log file name/path
    error_log_file = run_directory + '/' + 'error.log'

    # Create outpt script name/path
    ozone_create_commands_file = run_directory + '/' + 'ozoneCreateScript.sh'


    # Redirect print statements to both console and log file

    # Configure logging to both a file and the console
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler for logging to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Stream handler for printing to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)


    sys.stdout = PrintLogger(logger.info)
    sys.stderr = PrintLogger(logger.error)


    print "Run directory created: " + run_directory

    print "Log file name: " + log_file


    if fids_file:
        load_fids_from_input_file(fids_file)
        build_creation_input_json_list()
    elif creation_params_file:
        load_creation_input_file(creation_params_file)
        #print json.dumps(creation_input_json_list, indent=4, separators=(',', ': '))
        get_all_fids_from_creation_input_json_list()
    else:
        get_all_fids_from_hdfs()
        build_creation_input_json_list()


    buildOzoneCommands()

    if commands_string_list:
        writeOzoneCommandsFile()
        if not dry_run:
            executeOzoneCommandsFile()
