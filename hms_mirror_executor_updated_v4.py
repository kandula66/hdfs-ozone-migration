#!/usr/bin/env python3
"""
HMS Mirror Command Executor
Reads configuration from input file and executes hms-mirror commands dynamically
MODIFIED: Copies distcp script and config to STORAGE_MIGRATION folders
"""

import os
import sys
import subprocess
import argparse
import shutil
from datetime import datetime
from pathlib import Path


class HMSMirrorExecutor:
    def __init__(self, config_file, distcp_script=None, distcp_config=None):
        self.config_file = config_file
        self.distcp_script = distcp_script
        self.distcp_config = distcp_config
        self.config = {}
        self.output_dir = None
        self.log_file = None
        
    def parse_config(self):
        """Parse the input configuration file"""
        print(f"[INFO] Parsing configuration file: {self.config_file}")
        
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        
        with open(self.config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        # Remove quotes if present
                        value = value.strip('"').strip("'")
                        self.config[key.strip()] = value
        
        print(f"[INFO] Configuration loaded: {len(self.config)} parameters")
        return self.config
    
    def setup_output_directory(self):
        """Create output directory with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = f"hms_mirror_output_{timestamp}"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create log file
        self.log_file = os.path.join(self.output_dir, "execution.log")
        
        print(f"[INFO] Output directory created: {self.output_dir}")
        return self.output_dir
    
    def log(self, message):
        """Log message to both console and file"""
        print(message)
        if self.log_file:
            with open(self.log_file, 'a') as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    
    def extract_table_exclude_list(self):
        """Extract TABLES_EXCLUDE_LIST from config"""
        tables_exclude = self.config.get('TABLES_EXCLUDE_LIST', '')
        if tables_exclude:
            # Remove any existing quotes and whitespace
            tables_exclude = tables_exclude.strip('"').strip("'").strip()
        return tables_exclude
    
    def copy_distcp_files_to_storage_migration(self, hms_output_dir, db_name):
        """Copy distcp script and config to STORAGE_MIGRATION folder and update config"""
        try:
            # Expand $HOME in output_dir path
            output_dir_expanded = os.path.expandvars(hms_output_dir)
            
            if not os.path.exists(output_dir_expanded):
                self.log(f"[WARN] Output directory does not exist yet: {output_dir_expanded}")
                return False
            
            # Find the distcp source file in the HMS output directory
            distcp_source_file = None
            for file in os.listdir(output_dir_expanded):
                if file.endswith('_distcp_source.txt'):
                    distcp_source_file = os.path.join(output_dir_expanded, file)
                    break
            
            if not distcp_source_file:
                self.log(f"[WARN] No distcp source file found in {output_dir_expanded}")
                return False
            
            self.log(f"[INFO] Found distcp source file: {os.path.basename(distcp_source_file)}")
            
            # Copy distcp script if provided
            if self.distcp_script and os.path.exists(self.distcp_script):
                dest_script = os.path.join(output_dir_expanded, os.path.basename(self.distcp_script))
                shutil.copy2(self.distcp_script, dest_script)
                # Make it executable
                os.chmod(dest_script, 0o755)
                self.log(f"[INFO] Copied distcp script to: {dest_script}")
            else:
                self.log(f"[WARN] Distcp script not found or not provided: {self.distcp_script}")
                return False
            
            # Copy and update distcp config if provided
            if self.distcp_config and os.path.exists(self.distcp_config):
                dest_config = os.path.join(output_dir_expanded, os.path.basename(self.distcp_config))
                
                # Read the original config
                with open(self.distcp_config, 'r') as f:
                    config_content = f.read()
                
                # Update SOURCE_DISTCP_FILE path to point to the distcp source file in current directory
                # Extract just the filename from the full path
                distcp_source_filename = os.path.basename(distcp_source_file)
                
                # Replace the SOURCE_DISTCP_FILE line to use current directory path
                import re
                config_content = re.sub(
                    r'^SOURCE_DISTCP_FILE=.*$',
                    f'SOURCE_DISTCP_FILE="./{distcp_source_filename}"',
                    config_content,
                    flags=re.MULTILINE
                )
                
                # Write the updated config
                with open(dest_config, 'w') as f:
                    f.write(config_content)
                
                self.log(f"[INFO] Copied and updated distcp config to: {dest_config}")
                self.log(f"[INFO] Updated SOURCE_DISTCP_FILE=./{distcp_source_filename}")
            else:
                self.log(f"[WARN] Distcp config not found or not provided: {self.distcp_config}")
                return False
            
            # Create a README with instructions
            readme_path = os.path.join(output_dir_expanded, "DISTCP_README.txt")
            with open(readme_path, 'w') as f:
                f.write(f"DistCp Files for {db_name}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("Files in this directory:\n")
                f.write(f"  1. {os.path.basename(dest_script)} - DistCp execution script\n")
                f.write(f"  2. {os.path.basename(dest_config)} - DistCp configuration\n")
                f.write(f"  3. {distcp_source_filename} - Source file list\n\n")
                f.write("To execute DistCp:\n\n")
                f.write(f"  1. Navigate to this directory:\n")
                f.write(f"     cd {output_dir_expanded}\n\n")
                f.write(f"  2. Run the DistCp script:\n")
                f.write(f"     ./{os.path.basename(dest_script)} {os.path.basename(dest_config)}\n\n")
                f.write("Note:\n")
                f.write(f"  - The config file points to ./{distcp_source_filename} (current directory)\n")
                f.write(f"  - All required files are in this directory, no need to copy to /tmp\n")
                f.write(f"  - Make sure to execute from this directory for paths to work correctly\n")
            
            self.log(f"[INFO] Created README: {readme_path}")
            
            return True
            
        except Exception as e:
            self.log(f"[ERROR] Failed to copy distcp files: {str(e)}")
            return False
    
    def build_commands(self):
        """Build hms-mirror commands dynamically based on configuration"""
        commands = []
        
        # Extract configuration values
        db_names = self.config.get('DB_NAME', '').split(',')
        ewd_names = self.config.get('EWD_NAME', '').split('|')
        hdfs_ewd_names = self.config.get('HDFS_EWD_NAME', '').split('|')
        oz_name = self.config.get('OZ_NAME', '')
        man_name = self.config.get('MAN_NAME', '')
        tables_exclude = self.extract_table_exclude_list()
        ozone_prefix = self.config.get('OZONE_PREFIX', 'ozone_')  # Database prefix for SCHEMA_ONLY
        
        # Clean up values
        db_names = [db.strip() for db in db_names if db.strip()]
        ewd_names = [ewd.strip() for ewd in ewd_names if ewd.strip()]
        hdfs_ewd_names = [hdfs.strip() for hdfs in hdfs_ewd_names if hdfs.strip()]
        ozone_prefix = ozone_prefix.strip()
        
        # Create HMS Mirror session directory with timestamp (including seconds)
        hms_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        hms_session_dir = f"$HOME/.hms-mirror/reports/{hms_timestamp}"
        
        self.log(f"[INFO] Processing {len(db_names)} databases")
        self.log(f"[INFO] Using EWD path mapping strategy: one path per database")
        self.log(f"[INFO] HMS Mirror session directory: {hms_session_dir}")
        self.log(f"[INFO] Ozone database prefix: {ozone_prefix}")
        
        # Generate commands - one EWD path per database
        # Use the corresponding EWD path based on database index
        cmd_counter = 0
        for idx, db_name in enumerate(db_names):
            # Use corresponding EWD path or last one if index exceeds
            ewd_idx = idx if idx < len(ewd_names) else len(ewd_names) - 1
            ewd = ewd_names[ewd_idx]
            
            # Get corresponding HDFS_EWD_NAME
            hdfs_ewd = hdfs_ewd_names[ewd_idx] if ewd_idx < len(hdfs_ewd_names) else hdfs_ewd_names[-1]
            
            # Build global location map
            glm = f"{hdfs_ewd}={ewd}"
            
            self.log(f"[INFO] DB: {db_name} -> EWD: {ewd}, HDFS: {hdfs_ewd}")
            
            # Command 1: STORAGE_MIGRATION
            cmd_counter += 1
            hms_output_dir_storage = f"{hms_session_dir}/{db_name}_STORAGE_MIGRATION"
            cmd1 = self._build_storage_migration_command(
                db_name, ewd, oz_name, glm, tables_exclude, hms_output_dir_storage
            )
            commands.append({
                'type': 'STORAGE_MIGRATION',
                'db': db_name,
                'ewd': ewd,
                'command': cmd1,
                'hms_output_dir': hms_output_dir_storage,
                'hms_session_dir': hms_session_dir
            })
            
            # Command 2: SCHEMA_ONLY
            cmd_counter += 1
            hms_output_dir_schema = f"{hms_session_dir}/{db_name}_SCHEMA_ONLY"
            cmd2 = self._build_schema_only_command(
                db_name, ewd, oz_name, glm, tables_exclude, hms_output_dir_schema, ozone_prefix
            )
            commands.append({
                'type': 'SCHEMA_ONLY',
                'db': db_name,
                'ewd': ewd,
                'command': cmd2,
                'hms_output_dir': hms_output_dir_schema,
                'hms_session_dir': hms_session_dir
            })
        
        self.log(f"[INFO] Generated {len(commands)} commands")
        self.log(f"[INFO] Each command will create its own subdirectory under: {hms_session_dir}")
        return commands
    
    def _build_storage_migration_command(self, db_name, ewd, oz_name, glm, tables_exclude, output_dir):
        """Build STORAGE_MIGRATION command"""
        cmd_parts = [
            "hms-mirror -accept",
            "  --hadoop-classpath",
            "  -cfg /root/.hms-mirror/cfg/2025-11-12_23-02-06_Storage_Migration.yaml",
            "  -d STORAGE_MIGRATION",
            f"  -dbRegEx '^({db_name})$'",
        ]
        
        # Add table exclude filter if present
        if tables_exclude:
            cmd_parts.append(f"  -tef '^({tables_exclude})$'")
        
        cmd_parts.extend([
            f"  -ewd {ewd}",
            f"  -wd {ewd.rsplit('/', 1)[0]}/tmp" if '/' in ewd else "  -wd /tmp",
            f"  -smn {oz_name}",
            f"  -glm {glm}",
            "  -pol default",
            f"  -o {output_dir}"
        ])
        
        return " \\\n".join(cmd_parts)
    
    def _build_schema_only_command(self, db_name, ewd, oz_name, glm, tables_exclude, output_dir, db_prefix='ozone_'):
        """Build SCHEMA_ONLY command"""
        cmd_parts = [
            "hms-mirror -accept",
            "  --hadoop-classpath",
            "  -cfg /root/.hms-mirror/cfg/2025-11-13_00-03-02_schema_only_latest_nov13.yaml",
            "  -d SCHEMA_ONLY",
            f"  -dbRegEx '^({db_name})$'",
        ]
        
        # Add database prefix ONLY if provided and not empty
        if db_prefix and db_prefix.strip():
            cmd_parts.append(f"  -dbp {db_prefix}")
        
        # Add table exclude filter if present
        if tables_exclude:
            cmd_parts.append(f"  -tef '^({tables_exclude})$'")
        
        cmd_parts.extend([
            f"  -ewd {ewd}",
            f"  -wd {ewd.rsplit('/', 1)[0]}/tmp" if '/' in ewd else "  -wd /tmp",
            f"  -smn {oz_name}",
            f"  -glm {glm}",
            "  -pol default",
            f"  -o {output_dir}"
        ])
        
        return " \\\n".join(cmd_parts)
    
    def save_commands(self, commands):
        """Save generated commands to a file"""
        cmd_file = os.path.join(self.output_dir, "generated_commands.sh")
        
        # Get HMS session directory from first command
        hms_session_dir = commands[0].get('hms_session_dir', '$HOME/.hms-mirror/reports') if commands else '$HOME/.hms-mirror/reports'
        
        with open(cmd_file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write(f"# Generated HMS Mirror Commands\n")
            f.write(f"# Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Source config: {self.config_file}\n")
            f.write(f"# HMS Mirror session directory: {hms_session_dir}\n")
            f.write(f"# Note: Each command creates a subdirectory under the session directory\n")
            f.write(f"#       Format: {hms_session_dir}/<database>_<TYPE>\n\n")
            
            for idx, cmd_info in enumerate(commands, 1):
                f.write(f"\n# Command {idx}: {cmd_info['type']} - DB: {cmd_info['db']}, EWD: {cmd_info['ewd']}\n")
                f.write(f"# Output: {cmd_info['hms_output_dir']}\n")
                f.write(f"{cmd_info['command']}\n\n")
                f.write(f"echo 'Command {idx} completed with exit code: $?'\n")
                f.write(f"echo '-----------------------------------'\n\n")
        
        # Make the script executable
        os.chmod(cmd_file, 0o755)
        
        self.log(f"[INFO] Commands saved to: {cmd_file}")
        self.log(f"[INFO] HMS Mirror session directory: {hms_session_dir}")
        self.log(f"[INFO] Each command will create its own subdirectory")
        return cmd_file
    
    def execute_commands(self, commands, validate_mode=False):
        """Execute the generated commands"""
        results = []
        
        # Extract all paths from config
        hdfs_ewd_names = self.config.get('HDFS_EWD_NAME', '').split('|')
        ewd_names = self.config.get('EWD_NAME', '').split('|')
        prefix = self.config.get('OZONE_PREFIX', 'ozone_')  # Default prefix to remove
        
        for idx, cmd_info in enumerate(commands, 1):
            self.log(f"\n[INFO] ==================== Command {idx}/{len(commands)} ====================")
            self.log(f"[INFO] Type: {cmd_info['type']}")
            self.log(f"[INFO] Database: {cmd_info['db']}")
            self.log(f"[INFO] EWD Path: {cmd_info['ewd']}")
            self.log(f"\n[COMMAND]\n{cmd_info['command']}\n")
            
            if validate_mode:
                self.log("[INFO] VALIDATION MODE - Command not executed")
                results.append({
                    'command_id': idx,
                    'type': cmd_info['type'],
                    'db': cmd_info['db'],
                    'status': 'VALIDATED',
                    'exit_code': None
                })
                continue
            
            # Execute the command
            try:
                # Convert multi-line command to single line for execution
                cmd_single_line = cmd_info['command'].replace('\\\n', ' ')
                
                # Create output file for this command
                output_file = os.path.join(
                    self.output_dir, 
                    f"cmd_{idx}_{cmd_info['type']}_{cmd_info['db']}.log"
                )
                
                self.log(f"[INFO] Executing command... (output: {output_file})")
                
                with open(output_file, 'w') as out_f:
                    process = subprocess.Popen(
                        cmd_single_line,
                        shell=True,
                        stdout=out_f,
                        stderr=subprocess.STDOUT,
                        universal_newlines=True
                    )
                    
                    exit_code = process.wait()
                
                status = "SUCCESS" if exit_code == 0 else "FAILED"
                self.log(f"[INFO] Command completed with exit code: {exit_code} - {status}")
                
                # Post-process SQL files for STORAGE_MIGRATION commands
                if cmd_info['type'] == 'STORAGE_MIGRATION' and exit_code == 0:
                    self.log(f"[INFO] Post-processing SQL files for STORAGE_MIGRATION output...")
                    sql_processed = self._post_process_storage_migration_sql(
                        cmd_info['hms_output_dir']
                    )
                    if sql_processed:
                        self.log(f"[INFO] SQL post-processing completed: {sql_processed} files processed")
                    else:
                        self.log(f"[INFO] No SQL files found or processing skipped")
                    
                    # Copy distcp files to STORAGE_MIGRATION folder
                    self.log(f"[INFO] Copying distcp files to STORAGE_MIGRATION folder...")
                    if self.copy_distcp_files_to_storage_migration(cmd_info['hms_output_dir'], cmd_info['db']):
                        self.log(f"[INFO] Distcp files copied successfully")
                    else:
                        self.log(f"[WARN] Failed to copy distcp files")
                
                # Post-process SQL files for SCHEMA_ONLY commands
                if cmd_info['type'] == 'SCHEMA_ONLY' and exit_code == 0:
                    self.log(f"[INFO] Post-processing SQL files for SCHEMA_ONLY output...")
                    
                    # Find the correct data_dir and target_dir based on this command's EWD path
                    cmd_ewd = cmd_info['ewd'].strip()
                    ewd_names_stripped = [ewd.strip() for ewd in ewd_names]
                    
                    try:
                        ewd_index = ewd_names_stripped.index(cmd_ewd)
                    except ValueError:
                        # If not found, use the last one
                        ewd_index = len(ewd_names_stripped) - 1
                    
                    # Get corresponding HDFS path (or use last one if index exceeds)
                    if ewd_index < len(hdfs_ewd_names):
                        data_dir = hdfs_ewd_names[ewd_index].strip()
                    else:
                        data_dir = hdfs_ewd_names[-1].strip()
                    
                    target_dir = cmd_ewd
                    
                    self.log(f"[INFO] Using path mapping: {data_dir} -> {target_dir}")
                    
                    sql_processed = self._post_process_sql_files(
                        cmd_info['hms_output_dir'],
                        data_dir,
                        target_dir,
                        prefix
                    )
                    if sql_processed:
                        self.log(f"[INFO] SQL post-processing completed: {sql_processed} files processed")
                    else:
                        self.log(f"[INFO] No SQL files found or processing skipped")
                
                results.append({
                    'command_id': idx,
                    'type': cmd_info['type'],
                    'db': cmd_info['db'],
                    'status': status,
                    'exit_code': exit_code,
                    'output_file': output_file
                })
                
            except Exception as e:
                self.log(f"[ERROR] Command execution failed: {str(e)}")
                results.append({
                    'command_id': idx,
                    'type': cmd_info['type'],
                    'db': cmd_info['db'],
                    'status': 'ERROR',
                    'exit_code': -1,
                    'error': str(e)
                })
        
        return results
    
    def _post_process_storage_migration_sql(self, output_dir):
        """Post-process SQL files generated by STORAGE_MIGRATION command to remove MANAGEDLOCATION statements"""
        try:
            # Expand $HOME in output_dir path
            output_dir_expanded = os.path.expandvars(output_dir)
            
            if not os.path.exists(output_dir_expanded):
                self.log(f"[WARN] Output directory does not exist yet: {output_dir_expanded}")
                return 0
            
            # Find all .sql files in the output directory
            sql_files = []
            for root, dirs, files in os.walk(output_dir_expanded):
                for file in files:
                    if file.endswith('.sql') and not file.endswith('_cleaned.sql'):
                        sql_files.append(os.path.join(root, file))
            
            if not sql_files:
                return 0
            
            processed_count = 0
            for sql_file in sql_files:
                try:
                    # Create output filename
                    output_file = sql_file.replace('.sql', '_cleaned.sql')
                    
                    # Read input file
                    with open(sql_file, 'r') as f:
                        lines = f.readlines()
                    
                    # Filter out MANAGEDLOCATION lines and comment lines about it
                    filtered_lines = []
                    skip_next = False
                    
                    for i, line in enumerate(lines):
                        # Skip comment lines that mention "Managed Location"
                        if 'Managed Location' in line or 'MANAGEDLOCATION' in line.upper():
                            # Check if this is a comment line
                            if line.strip().startswith('--'):
                                continue
                            # Check if this is an ALTER DATABASE MANAGEDLOCATION statement
                            elif 'MANAGEDLOCATION' in line.upper():
                                continue
                        
                        filtered_lines.append(line)
                    
                    # Write cleaned content
                    with open(output_file, 'w') as f:
                        f.writelines(filtered_lines)
                    
                    self.log(f"[INFO] Cleaned SQL: {os.path.basename(sql_file)} -> {os.path.basename(output_file)}")
                    processed_count += 1
                    
                except Exception as e:
                    self.log(f"[ERROR] Failed to process {sql_file}: {str(e)}")
            
            return processed_count
            
        except Exception as e:
            self.log(f"[ERROR] SQL post-processing failed: {str(e)}")
            return 0
    
    def _post_process_sql_files(self, output_dir, data_dir, target_dir, prefix):
        """Post-process SQL files generated by SCHEMA_ONLY command"""
        try:
            # Expand $HOME in output_dir path
            output_dir_expanded = os.path.expandvars(output_dir)
            
            if not os.path.exists(output_dir_expanded):
                self.log(f"[WARN] Output directory does not exist yet: {output_dir_expanded}")
                return 0
            
            # Find all .sql files in the output directory
            sql_files = []
            for root, dirs, files in os.walk(output_dir_expanded):
                for file in files:
                    if file.endswith('.sql') and not file.endswith('_transformed.sql'):
                        sql_files.append(os.path.join(root, file))
            
            if not sql_files:
                return 0
            
            processed_count = 0
            for sql_file in sql_files:
                try:
                    # Create output filename
                    output_file = sql_file.replace('.sql', '_transformed.sql')
                    
                    # Read input file
                    with open(sql_file, 'r') as f:
                        content = f.read()
                    
                    # Apply transformations
                    import re
                    
                    # Process ALTER DATABASE SET LOCATION lines and LOCATION lines after CREATE DATABASE
                    def transform_location_path(match):
                        db_name = match.group(1) if match.lastindex >= 1 else None
                        location_path = match.group(2) if match.lastindex >= 2 else match.group(1)
                        
                        # Replace full HDFS_EWD_NAME path with EWD_NAME path
                        # e.g., /data/fid1/raw -> /fid1/raw
                        location_path = location_path.replace(data_dir, target_dir)
                        
                        # Remove prefix from path (e.g., /ozone_hdfs_db1 -> /hdfs_db1)
                        # Only process if prefix is not empty and has actual content
                        if prefix and prefix.strip():
                            location_path = location_path.replace(f"/{prefix}", "/")
                        
                        if db_name:
                            return f'ALTER DATABASE {db_name} SET LOCATION "{location_path}";'
                        else:
                            return f'LOCATION "{location_path}";'
                    
                    # FIRST: Remove all comment lines to simplify processing
                    lines = content.split('\n')
                    lines = [line for line in lines if not line.strip().startswith('-- ')]
                    content = '\n'.join(lines)
                    
                    # SECOND: Remove COMMENT lines from CREATE DATABASE
                    lines = content.split('\n')
                    lines = [line for line in lines if not line.strip().startswith('COMMENT "')]
                    content = '\n'.join(lines)
                    
                    # THIRD: Match and transform ALTER DATABASE SET LOCATION lines (update paths)
                    content = re.sub(
                        r'ALTER DATABASE\s+(\S+)\s+SET LOCATION\s+"([^"]+)";?',
                        transform_location_path,
                        content,
                        flags=re.MULTILINE
                    )
                    
                    # FOURTH: Match and transform standalone LOCATION lines (after CREATE DATABASE)
                    content = re.sub(
                        r'^LOCATION\s+"([^"]+)";?$',
                        transform_location_path,
                        content,
                        flags=re.MULTILINE
                    )
                    
                    # FIFTH: Now combine CREATE DATABASE + ALTER DATABASE into single statement
                    # This needs to match after comments are removed
                    # Pattern: CREATE DATABASE ... ; (whitespace/newlines) ALTER DATABASE ... SET LOCATION
                    content = re.sub(
                        r'(CREATE DATABASE[^;]*);?\s*\n+\s*ALTER DATABASE\s+(\S+)\s+SET LOCATION\s+"([^"]+)";?',
                        lambda m: f'{m.group(1)}\nLOCATION "{m.group(3)}";',
                        content,
                        flags=re.MULTILINE | re.DOTALL
                    )
                    
                    # SIXTH: Handle standalone ALTER DATABASE (when no CREATE DATABASE exists)
                    # This is the case for RIGHT_execute files when prefix is empty
                    # Convert: ALTER DATABASE dbname SET LOCATION "path";
                    # To: CREATE DATABASE IF NOT EXISTS dbname\nLOCATION "path";
                    def convert_alter_to_create(match):
                        db_name = match.group(1)
                        location_path = match.group(2)
                        return f'CREATE DATABASE IF NOT EXISTS {db_name}\nLOCATION "{location_path}";'
                    
                    # Only do this conversion if there's no CREATE DATABASE in the content
                    if 'CREATE DATABASE' not in content:
                        content = re.sub(
                            r'^ALTER DATABASE\s+(\S+)\s+SET LOCATION\s+"([^"]+)";?',
                            convert_alter_to_create,
                            content,
                            flags=re.MULTILINE
                        )
                        self.log(f"[INFO] Converted standalone ALTER DATABASE to CREATE DATABASE in {os.path.basename(sql_file)}")
                    
                    # SEVENTH: Remove blank lines between CREATE DATABASE and LOCATION
                    content = re.sub(
                        r'(CREATE DATABASE[^\n]*)\n+(\s*LOCATION)',
                        r'\1\n\2',
                        content
                    )
                    
                    # EIGHTH: Remove extra blank lines (keep max 1)
                    content = re.sub(r'\n{3,}', '\n\n', content)
                    
                    # Write transformed content
                    with open(output_file, 'w') as f:
                        f.write(content)
                    
                    self.log(f"[INFO] Transformed SQL: {os.path.basename(sql_file)} -> {os.path.basename(output_file)}")
                    processed_count += 1
                    
                except Exception as e:
                    self.log(f"[ERROR] Failed to process {sql_file}: {str(e)}")
            
            return processed_count
            
        except Exception as e:
            self.log(f"[ERROR] SQL post-processing failed: {str(e)}")
            return 0
    
    def generate_summary(self, results):
        """Generate execution summary"""
        summary_file = os.path.join(self.output_dir, "execution_summary.txt")
        
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("HMS MIRROR EXECUTION SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Configuration File: {self.config_file}\n")
            f.write(f"Output Directory: {self.output_dir}\n")
            f.write(f"Total Commands: {len(results)}\n\n")
            
            # Count statistics
            success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
            failed_count = sum(1 for r in results if r['status'] == 'FAILED')
            error_count = sum(1 for r in results if r['status'] == 'ERROR')
            validated_count = sum(1 for r in results if r['status'] == 'VALIDATED')
            
            f.write("Statistics:\n")
            f.write(f"  - Successful: {success_count}\n")
            f.write(f"  - Failed: {failed_count}\n")
            f.write(f"  - Errors: {error_count}\n")
            if validated_count > 0:
                f.write(f"  - Validated: {validated_count}\n")
            f.write("\n" + "-" * 80 + "\n\n")
            
            f.write("Detailed Results:\n\n")
            for result in results:
                f.write(f"Command {result['command_id']}:\n")
                f.write(f"  Type: {result['type']}\n")
                f.write(f"  Database: {result['db']}\n")
                f.write(f"  Status: {result['status']}\n")
                if result.get('exit_code') is not None:
                    f.write(f"  Exit Code: {result['exit_code']}\n")
                if 'output_file' in result:
                    f.write(f"  Output: {result['output_file']}\n")
                if 'error' in result:
                    f.write(f"  Error: {result['error']}\n")
                f.write("\n")
        
        self.log(f"\n[INFO] Summary saved to: {summary_file}")
        
        # Print summary to console
        print("\n" + "=" * 80)
        print("EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Total Commands: {len(results)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {failed_count}")
        print(f"Errors: {error_count}")
        if validated_count > 0:
            print(f"Validated: {validated_count}")
        print(f"\nFull summary: {summary_file}")
        print(f"Output directory: {self.output_dir}")
        print("=" * 80)
    
    def run(self, validate_mode=False, generate_scripts=True):
        """Main execution flow"""
        try:
            # Parse configuration
            self.parse_config()
            
            # Setup output directory
            self.setup_output_directory()
            
            # Build commands
            commands = self.build_commands()
            
            # Save commands to file
            cmd_file = self.save_commands(commands)
            
            # Execute commands if requested
            if generate_scripts:
                results = self.execute_commands(commands, validate_mode=validate_mode)
                self.generate_summary(results)
            else:
                self.log("\n[INFO] Commands generated but not executed (use --generate-scripts to run)")
                self.log(f"[INFO] You can manually execute: {cmd_file}")
            
            self.log("\n" + "=" * 80)
            unprocessed_parent_folders = self.find_unprocessed_parent_folders()
            self.save_unprocessed_parent_folders_report(unprocessed_parent_folders)
            return True
            
        except Exception as e:
            self.log(f"[ERROR] Execution failed: {str(e)}")
            import traceback
            self.log(f"[ERROR] Traceback:\n{traceback.format_exc()}")
            return False

# =============================================================================
# UPDATED PATCH FOR hms_mirror_executor_updated_v3.py
# Detect child folders at parent level (before /hive)
# =============================================================================

# ADD THESE METHODS TO THE HMSMirrorExecutor CLASS:

    def get_hdfs_subdirectories(self, hdfs_path):
        """Get list of subdirectories in an HDFS path using hdfs dfs -ls"""
        try:
            # Run hdfs dfs -ls command
            cmd = f"hdfs dfs -ls {hdfs_path}"
            
            # Python 3.6 compatible - use Popen instead of run with capture_output
            import subprocess
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            stdout, stderr = process.communicate(timeout=30)
            returncode = process.returncode
            
            if returncode != 0:
                self.log(f"[WARN] Failed to list HDFS path: {hdfs_path}")
                self.log(f"[WARN] Error: {stderr}")
                return []
            
            # Parse output to extract directory names
            subdirs = []
            for line in stdout.strip().split('\n'):
                # Skip empty lines and header
                if not line.strip() or line.startswith('Found'):
                    continue
                
                # Check if it's a directory (starts with 'd')
                if line.startswith('d'):
                    # Extract path from the line (last column)
                    parts = line.split()
                    if len(parts) >= 8:
                        full_path = parts[-1]
                        # Extract just the directory name
                        dir_name = full_path.split('/')[-1]
                        if dir_name:
                            subdirs.append(dir_name)
            
            return subdirs
            
        except subprocess.TimeoutExpired:
            self.log(f"[ERROR] Timeout while listing HDFS path: {hdfs_path}")
            return []
        except Exception as e:
            self.log(f"[ERROR] Failed to get subdirectories from {hdfs_path}: {str(e)}")
            return []    
    def extract_parent_path(self, hdfs_path):
        """
        Extract parent path before /hive directory
        Example: /data/fid2/raw/hive -> /data/fid2/raw
        """
        # Remove trailing slash
        hdfs_path = hdfs_path.rstrip('/')
        
        # Split path and find parent
        if '/hive' in hdfs_path:
            parent = hdfs_path.rsplit('/hive', 1)[0]
        elif '/managed' in hdfs_path:
            parent = hdfs_path.rsplit('/managed', 1)[0]
        elif '/raw' in hdfs_path:
            parent = hdfs_path.rsplit('/raw', 1)[0]
        else:
            # If no known pattern, go up one level
            parent = hdfs_path.rsplit('/', 1)[0]
        
        return parent
    
    def find_unprocessed_parent_folders(self):
        """
        Find child folders at the PARENT level (before /hive)
        
        Example:
          HDFS_EWD_NAME="/data/fid2/raw/hive|/data/fid2/managed/hive"
          
          Scans:
            /data/fid2/raw/           <- parent of /hive
            /data/fid2/managed/       <- parent of /hive
          
          Finds folders like:
            /data/fid2/raw/hive       <- being processed
            /data/fid2/raw/spark      <- NOT processed (reported)
            /data/fid2/raw/test       <- NOT processed (reported)
        """
        self.log("[INFO] ===== Scanning for Unprocessed Parent-Level Folders =====")
        
        # Get configuration values
        hdfs_ewd_names = self.config.get('HDFS_EWD_NAME', '').split('|')
        
        # Clean up values
        hdfs_ewd_names = [path.strip() for path in hdfs_ewd_names if path.strip()]
        
        if not hdfs_ewd_names:
            self.log("[WARN] No HDFS_EWD_NAME paths configured")
            return {}
        
        # Extract unique parent paths
        parent_paths = set()
        processed_folders = set()
        
        for hdfs_path in hdfs_ewd_names:
            parent = self.extract_parent_path(hdfs_path)
            parent_paths.add(parent)
            
            # Extract the folder name being processed (e.g., "hive" from "/data/fid2/raw/hive")
            folder_name = hdfs_path.rstrip('/').split('/')[-1]
            processed_folders.add(folder_name.lower())
        
        self.log(f"[INFO] Scanning {len(parent_paths)} parent paths")
        self.log(f"[INFO] Folders being processed: {', '.join(sorted(processed_folders))}")
        
        # Dictionary to store unprocessed folders per parent path
        unprocessed_folders = {}
        
        for parent_path in sorted(parent_paths):
            self.log(f"\n[INFO] Scanning parent path: {parent_path}")
            
            # Add hdfs://ns1 prefix if not present
            if not parent_path.startswith('hdfs://'):
                full_path = f"hdfs://ns1{parent_path}"
            else:
                full_path = parent_path
            
            # Get subdirectories
            subdirs = self.get_hdfs_subdirectories(full_path)
            
            if not subdirs:
                self.log(f"[INFO] No subdirectories found in {parent_path}")
                continue
            
            self.log(f"[INFO] Found {len(subdirs)} subdirectories in parent")
            
            # Find subdirectories that are NOT in the processed folders list
            unprocessed = []
            for subdir in subdirs:
                subdir_lower = subdir.lower()
                if subdir_lower not in processed_folders:
                    unprocessed.append(subdir)
                    self.log(f"[INFO]   Unprocessed: {subdir}")
                else:
                    self.log(f"[INFO]   Processing: {subdir} (in HDFS_EWD_NAME)")
            
            if unprocessed:
                unprocessed_folders[parent_path] = sorted(unprocessed)
                self.log(f"[INFO] Total unprocessed in {parent_path}: {len(unprocessed)}")
            else:
                self.log(f"[INFO] All subdirectories in {parent_path} are being processed")
        
        return unprocessed_folders
    
    def save_unprocessed_parent_folders_report(self, unprocessed_folders):
        """Save unprocessed parent-level folders report to a file"""
        if not unprocessed_folders:
            self.log("\n[INFO] No unprocessed parent folders found - all subdirectories are being processed")
            return None
        
        report_file = os.path.join(self.output_dir, "unprocessed_parent_folders_report.txt")
        
        try:
            with open(report_file, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("UNPROCESSED PARENT-LEVEL FOLDERS REPORT\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Configuration: {self.config_file}\n\n")
                
                f.write("Summary:\n")
                total_unprocessed = sum(len(folders) for folders in unprocessed_folders.values())
                f.write(f"  Total unprocessed folders: {total_unprocessed}\n")
                f.write(f"  Parent paths scanned: {len(unprocessed_folders)}\n\n")
                
                f.write("-" * 80 + "\n\n")
                
                # Paths being processed
                hdfs_ewd_names = self.config.get('HDFS_EWD_NAME', '').split('|')
                hdfs_ewd_names = [path.strip() for path in hdfs_ewd_names if path.strip()]
                f.write("HDFS Paths Being Processed (from HDFS_EWD_NAME):\n")
                for path in sorted(hdfs_ewd_names):
                    f.write(f"  - {path}\n")
                f.write("\n" + "-" * 80 + "\n\n")
                
                # Unprocessed folders by parent path
                f.write("Unprocessed Folders by Parent Path:\n\n")
                for parent_path, folders in sorted(unprocessed_folders.items()):
                    f.write(f"Parent Path: {parent_path}\n")
                    f.write(f"  Unprocessed count: {len(folders)}\n")
                    f.write(f"  Folders:\n")
                    for folder in sorted(folders):
                        f.write(f"    - {folder}\n")
                    f.write("\n")
                
                f.write("-" * 80 + "\n\n")
                
                # Full paths for easy copying
                f.write("Full Paths (for reference):\n\n")
                for parent_path, folders in sorted(unprocessed_folders.items()):
                    for folder in sorted(folders):
                        # Add hdfs://ns1 prefix if not present
                        if parent_path.startswith('hdfs://'):
                            full_path = f"{parent_path}/{folder}"
                        else:
                            full_path = f"hdfs://ns1{parent_path}/{folder}"
                        f.write(f"{full_path}\n")
                
                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")
            
            self.log(f"\n[INFO] Unprocessed parent folders report saved to: {report_file}")
            
            # Also create a simple list file for easy scripting
            list_file = os.path.join(self.output_dir, "unprocessed_parent_folders_list.txt")
            with open(list_file, 'w') as f:
                f.write("# Unprocessed parent-level folders (one per line)\n")
                f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                for parent_path, folders in sorted(unprocessed_folders.items()):
                    f.write(f"# Parent path: {parent_path}\n")
                    for folder in sorted(folders):
                        f.write(f"{folder}\n")
                    f.write("\n")
            
            self.log(f"[INFO] Unprocessed parent folders list saved to: {list_file}")
            
            # Print summary to console
            print("\n" + "=" * 80)
            print("UNPROCESSED PARENT-LEVEL FOLDERS SUMMARY")
            print("=" * 80)
            print(f"Total unprocessed folders: {total_unprocessed}")
            print(f"Parent paths scanned: {len(unprocessed_folders)}")
            print("\nDetails:")
            for parent_path, folders in sorted(unprocessed_folders.items()):
                print(f"  {parent_path}: {len(folders)} unprocessed folders")
                for folder in sorted(folders)[:5]:  # Show first 5
                    print(f"    - {folder}")
                if len(folders) > 5:
                    print(f"    ... and {len(folders) - 5} more")
            print(f"\nFull report: {report_file}")
            print(f"Simple list: {list_file}")
            print("=" * 80)
            
            return report_file
            
        except Exception as e:
            self.log(f"[ERROR] Failed to save unprocessed parent folders report: {str(e)}")
            return None
def main():
    parser = argparse.ArgumentParser(
        description='HMS Mirror Command Generator and Executor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate commands only (no execution)
  python hms_mirror_executor.py -c input_file.conf
  
  # Generate and execute commands
  python hms_mirror_executor.py -c input_file.conf --generate-scripts
  
  # Generate and execute with distcp files
  python hms_mirror_executor.py -c input_file.conf --generate-scripts \\
      --distcp-script hdfs2ozone_inter_cluster.sh \\
      --distcp-config input_param.conf
  
  # Validation mode (show what would be executed)
  python hms_mirror_executor.py -c input_file.conf --generate-scripts --validate-hms-mirror-cmds
        """
    )
    
    parser.add_argument(
        '-c', '--config',
        required=True,
        help='Path to input configuration file'
    )
    
    parser.add_argument(
        '--generate-scripts',
        action='store_true',
        help='Execute the generated commands (default: only generate)'
    )
    
    parser.add_argument(
        '--validate-hms-mirror-cmds',
        action='store_true',
        help='Validation mode - show commands without executing'
    )
    
    parser.add_argument(
        '--distcp-script',
        help='Path to distcp script file (e.g., hdfs2ozone_inter_cluster.sh) to copy to STORAGE_MIGRATION folders'
    )
    
    parser.add_argument(
        '--distcp-config',
        help='Path to distcp config file (e.g., input_param.conf) to copy and update in STORAGE_MIGRATION folders'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("HMS MIRROR COMMAND EXECUTOR")
    print("=" * 80)
    print()
    
    # Validate distcp files if provided
    if args.distcp_script and not os.path.exists(args.distcp_script):
        print(f"ERROR: Distcp script file not found: {args.distcp_script}")
        sys.exit(1)
    
    if args.distcp_config and not os.path.exists(args.distcp_config):
        print(f"ERROR: Distcp config file not found: {args.distcp_config}")
        sys.exit(1)
    
    executor = HMSMirrorExecutor(args.config, args.distcp_script, args.distcp_config)
    success = executor.run(validate_mode=args.validate_hms_mirror_cmds, generate_scripts=args.generate_scripts)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
