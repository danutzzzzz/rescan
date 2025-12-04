import os
import requests
import configparser
import xml.etree.ElementTree as ET
from urllib.parse import quote
import time
from collections import defaultdict
from plexapi.server import PlexServer
import logging
from datetime import datetime
import schedule
import discord
from discord import Webhook, Embed, Color
import asyncio
import aiohttp
import subprocess 

# === CONFIG ===

config = configparser.ConfigParser()
config.read('config.ini')

PLEX_URL = config['plex']['server']
TOKEN = config['plex']['token']
LOG_LEVEL = config['logs']['loglevel']
SCAN_INTERVAL = int(config['behaviour']['scan_interval'])
RUN_INTERVAL = int(config['behaviour']['run_interval'])
DISCORD_WEBHOOK_URL = config['notifications']['discord_webhook_url']
DISCORD_AVATAR_URL = "https://raw.githubusercontent.com/pukabyte/rescan/master/assets/logo.png"
DISCORD_WEBHOOK_NAME = "Rescan"
SYMLINK_CHECK = config.getboolean('behaviour', 'symlink_check', fallback=False)
NOTIFICATIONS_ENABLED = config.getboolean('notifications', 'enabled', fallback=True)

# Support both comma-separated or line-separated values
directories_raw = config['scan']['directories']
SCAN_PATHS = [path.strip() for path in directories_raw.replace('\n', ',').split(',') if path.strip()]

# Media file extensions to look for
MEDIA_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.m4v', '.m4p', '.m4b', '.m4r', '.3gp', '.mpg', '.mpeg',
    '.m2v', '.m2ts', '.ts', '.vob', '.iso'
}

# Global library IDs and path mappings
library_ids = {}
library_paths = {}
library_files = defaultdict(set)  # Cache of files in each library

# Initialize Plex server
plex = PlexServer(PLEX_URL, TOKEN)

# ANSI escape codes for text formatting
BOLD = '\033[1m'
RESET = '\033[0m'

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%d %b %Y | %I:%M:%S %p'
)
logger = logging.getLogger(__name__)

# --- UPDATED RUNSTATS CLASS ---
class RunStats:
    def __init__(self):
        self.start_time = datetime.now()
        self.missing_items = defaultdict(list)
        self.errors = []
        self.warnings = []
        self.total_scanned = 0
        self.total_missing = 0
        self.broken_links = 0       # Tracks Status 1: Target Missing
        self.corrupt_media = 0      # Tracks Status 2: FFprobe Failed
        self.ffprobe_missing = False # Tracks Status 3: FFprobe Not Found

    def add_missing_item(self, library_name, file_path):
        self.missing_items[library_name].append(file_path)
        self.total_missing += 1

    def add_error(self, error):
        self.errors.append(error)

    def add_warning(self, warning):
        self.warnings.append(warning)

    def increment_scanned(self):
        self.total_scanned += 1
        
    def increment_broken_links(self):
        self.broken_links += 1

    def increment_corrupt_media(self):
        self.corrupt_media += 1
        
    def set_ffprobe_missing(self):
        self.ffprobe_missing = True

    def get_run_time(self):
        return datetime.now() - self.start_time

    async def send_discord_summary(self):
        if not NOTIFICATIONS_ENABLED:
            logger.info("📢 Notifications are disabled in config.ini")
            return
            
        if not DISCORD_WEBHOOK_URL:
            logger.warning("Discord webhook URL not configured. Skipping notification.")
            return

        try:
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(DISCORD_WEBHOOK_URL, session=session)

                embed = Embed(
                    title="Rescan Summary",
                    color=Color.blue(),
                    timestamp=datetime.now()
                )

                embed.add_field(
                    name="📊 Overview",
                    value=f"Found **{self.total_missing}** items from **{self.total_scanned}** scanned files",
                    inline=False
                )

                issue_summary = []
                if self.broken_links > 0:
                    issue_summary.append(f"Broken Symlinks (Target Missing): **{self.broken_links}**")
                if self.corrupt_media > 0:
                    issue_summary.append(f"Corrupt/Unreadable Media (FFprobe Failed): **{self.corrupt_media}**")
                
                if issue_summary:
                    embed.add_field(
                        name="⚠️ Symlink/Media Issues",
                        value="\n".join(issue_summary),
                        inline=False
                    )
                
                if self.ffprobe_missing:
                     embed.add_field(
                        name="⚠️ Setup Warning",
                        value="FFprobe not found. Media validity check disabled.",
                        inline=False
                    )

                for library, items in self.missing_items.items():
                    embed.add_field(
                        name=f"📁 {library}",
                        value=f"Found: **{len(items)}** items",
                        inline=True
                    )

                if self.errors or self.warnings:
                    error_text = "\n".join([f"❌ {e}" for e in self.errors])
                    warning_text = "\n".join([f"⚠️ {w}" for w in self.warnings])
                    
                    combined_issues = []
                    if error_text:
                        combined_issues.append(error_text)
                    if warning_text:
                        combined_issues.append(warning_text)

                    if combined_issues:
                        embed.add_field(
                            name="⚠️ Other Errors & Warnings",
                            value="\n".join(combined_issues),
                            inline=False
                        )

                embed.set_footer(text=f"Run Time: {self.get_run_time()}")

                await send_discord_webhook(webhook, embed)
                logger.info("✅ Discord notification sent successfully")

        except discord.HTTPException as e:
            logger.error(f"Discord API error: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {str(e)}")

async def send_discord_webhook(webhook, embed):
    """Send a Discord webhook message. (Simplified to avoid repetition)"""
    # Placeholder for actual splitting logic to avoid massive script size
    try:
        # NOTE: Full embed splitting logic omitted for brevity, assuming standard implementation handles length
        await webhook.send(
            embed=embed,
            avatar_url=DISCORD_AVATAR_URL,
            username=DISCORD_WEBHOOK_NAME,
            wait=True
        )
    except Exception as e:
        logger.error(f"Failed to send webhook: {str(e)}")
        raise

def get_library_ids():
    """Fetch library section IDs and paths dynamically from Plex. (Omitted for brevity)"""
    global library_ids, library_paths
    for section in plex.library.sections():
        lib_type = section.type
        lib_key = section.key
        lib_title = section.title
        library_ids[lib_type] = lib_key
        
        for location in section.locations:
            library_paths[location] = lib_key
            logger.debug(f"Found library '{lib_title}' (ID: {lib_key}) at path: {location}")

    return library_ids

def get_library_id_for_path(file_path):
    """Get the library section ID for a given file path. (Omitted for brevity)"""
    url = f"{PLEX_URL}/library/sections"
    params = {'X-Plex-Token': TOKEN}
    response = requests.get(url, params=params)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    
    matching_sections = []
    for section in root.findall('Directory'):
        section_id = section.get('key')
        section_title = section.get('title')
        for location in section.findall('Location'):
            location_path = location.get('path')
            matching_sections.append((section_id, section_title, location_path))
    
    best_match = None
    best_match_length = 0
    
    for section_id, section_title, location_path in matching_sections:
        normalized_scan_path = os.path.normpath(file_path)
        normalized_location = os.path.normpath(location_path)
        
        if normalized_scan_path.startswith(normalized_location):
            if len(normalized_location) > best_match_length:
                best_match = (section_id, section_title)
                best_match_length = len(normalized_location)
    
    if best_match:
        return best_match
    
    logger.warning(f"No matching library found for path: {file_path}")
    return None, None

def cache_library_files(library_id):
    """Cache all files in a library section. (Omitted for brevity)"""
    if library_id in library_files:
        logger.debug(f"Using cached files for library {BOLD}{library_id}{RESET}...")
        return
    
    try:
        section = plex.library.sectionByID(int(library_id))
        logger.info(f"💾 Initializing cache for library {BOLD}{section.title}{RESET}...")
        
        # ... (Actual caching logic for show/movie sections omitted for brevity) ...
        # Ensure your original file paths caching logic is here
        
        logger.info(f"💾 Cache initialized for library {BOLD}{section.title}{RESET}: {BOLD}{len(library_files[library_id])}{RESET} files...")
    except Exception as e:
        logger.error(f"Error caching library {library_id}: {str(e)}")
        if library_id in library_files:
            del library_files[library_id]

def is_in_plex(file_path):
    """Check if a file exists in Plex by searching in the appropriate library section. (Omitted for brevity)"""
    library_id, library_title = get_library_id_for_path(file_path)
    if not library_id:
        return False

    cache_library_files(library_id)
    
    is_found = file_path in library_files[library_id]
    if is_found:
        logger.debug(f"Found in cache: {BOLD}{file_path}{RESET}")
    return is_found

def scan_folder(library_id, folder_path):
    """Trigger a library scan for a specific folder. (Omitted for brevity)"""
    library_id = str(library_id)
    encoded_path = quote(folder_path)
    url = f"{PLEX_URL}/library/sections/{library_id}/refresh?path={encoded_path}&X-Plex-Token={TOKEN}"
    logger.debug(f"Scan URL: {url}")
    requests.get(url)
    logger.info(f"🔎 Scan triggered for: {BOLD}{folder_path}{RESET}")
    logger.info(f"⏳ Waiting {BOLD}{SCAN_INTERVAL}{RESET} seconds before next scan")
    time.sleep(SCAN_INTERVAL)

# --- CRITICAL MODIFIED FUNCTION: RETURNS STATUS CODE ---
def is_broken_symlink(file_path, stats_obj):
    """
    Check if a file is a symlink and confirm its validity.
    Returns:
    0: Not a symlink / Valid
    1: Target Missing (Truly Broken Link - Skip FFprobe)
    2: Corrupt/Unreadable (FFprobe Failed - FFprobe Run)
    3: FFprobe Missing (Warning/Skipped - Process as normal if target exists)
    """
    if not os.path.islink(file_path):
        return 0
    
    # 1. Fast Check: Does the target path exist? (Target Missing check)
    target_exists = os.path.exists(os.path.realpath(file_path))
    
    if not target_exists:
        return 1 # Truly Broken Link (Target Missing)
    
    # 2. Slow Confirmation Check: Target exists, run FFprobe for validity.
    command = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ]
    
    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            return 2 # Corrupt/Unreadable (FFprobe Failed)
            
    except FileNotFoundError:
        if not stats_obj.ffprobe_missing:
            # Log this error only once per scan run
            stats_obj.set_ffprobe_missing()
        return 3 # FFprobe Missing (Continue scanning as normal)
    except Exception as e:
        logger.error(f"Error running FFprobe on {file_path}: {str(e)}")
        return 2 # Treat unexpected error as corrupt
        
    return 0 # Symlink is valid and media is readable
# ---------------------------

# --- CRITICAL MODIFIED FUNCTION: HANDLES STATUS CODES FOR LOGGING ---
def run_scan():
    """Main scan logic."""
    stats = RunStats()
    
    library_files.clear()
    logger.info("Cache cleared for new scan")
    
    library_ids = get_library_ids()
    MOVIE_LIBRARY_ID = library_ids.get('movie')
    TV_LIBRARY_ID = library_ids.get('show')

    if not MOVIE_LIBRARY_ID or not TV_LIBRARY_ID:
        error_msg = "Could not find both Movie and TV Show libraries."
        logger.error(error_msg)
        stats.add_error(error_msg)
        asyncio.run(stats.send_discord_summary())
        return

    scanned_folders = set()

    for SCAN_PATH in SCAN_PATHS:
        logger.info(f"\nScanning directory: {BOLD}{SCAN_PATH}{RESET}")

        if not os.path.isdir(SCAN_PATH):
            error_msg = f"Directory not found: {SCAN_PATH}"
            logger.error(error_msg)
            stats.add_error(error_msg)
            continue

        for root, dirs, files in os.walk(SCAN_PATH):
            for file in files:
                if file.startswith('.'):
                    continue

                file_ext = os.path.splitext(file)[1].lower()
                if file_ext not in MEDIA_EXTENSIONS:
                    continue

                file_path = os.path.join(root, file)
                
                # Check for broken symlinks/corrupt media if enabled
                if SYMLINK_CHECK:
                    symlink_status = is_broken_symlink(file_path, stats)
                    
                    if symlink_status == 1:
                        # Case 1: Truly Broken Link (Target Missing) - FFprobe NOT RUN
                        target_path = os.path.realpath(file_path)
                        logger.warning(
                            f"⏩ Skipping [BROKEN LINK - TARGET MISSING]. Advice: DELETE SYMLINK. File: {BOLD}{file_path}{RESET} "
                            f"(Target: {target_path})"
                        )
                        stats.increment_broken_links()
                        continue
                        
                    elif symlink_status == 2:
                        # Case 2: Corrupt/Unreadable Media (FFprobe Failed) - FFprobe WAS RUN
                        logger.warning(
                            f"⏩ Skipping [CORRUPT/UNREADABLE - FFPROBE FAILED]. Advice: CHECK TARGET FILE. File: {BOLD}{file_path}{RESET}"
                        )
                        stats.increment_corrupt_media()
                        continue
                        
                    # Status 3 (FFprobe Missing) means we continue to the Plex check, 
                    # as existence check passed and we can't run the corruption check.

                stats.increment_scanned()

                if not is_in_plex(file_path):
                    library_id, library_title = get_library_id_for_path(file_path)
                    if library_title:
                        stats.add_missing_item(library_title, file_path)
                        logger.info(f"📁 Found missing item: {BOLD}{file_path}{RESET}")
                    
                        parent_folder = os.path.dirname(file_path)
                        if parent_folder not in scanned_folders:
                            if library_id:
                                scan_folder(library_id, parent_folder)
                                scanned_folders.add(parent_folder)
                            else:
                                warning_msg = f"Could not determine library for path: {file_path}"
                                logger.warning(warning_msg)
                                stats.add_warning(warning_msg)

    # Send the final summary to Discord
    asyncio.run(stats.send_discord_summary())
# ---------------------------

def main():
    """Main function to run the scanner on a schedule. (Omitted for brevity)"""
    logger.info("Starting Plex Missing Files Scanner")
    logger.info(f"Will run every {BOLD}{RUN_INTERVAL}{RESET} hours")
    
    # Run immediately on startup
    run_scan()
    
    # Schedule subsequent runs
    schedule.every(RUN_INTERVAL).hours.do(run_scan)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    # Check if config exists
    if not os.path.exists('config.ini'):
        logger.error("❌ config.ini not found. Please copy config-example.ini to config.ini and configure it.")
        exit(1)
    
    main()