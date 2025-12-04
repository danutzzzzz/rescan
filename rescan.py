import os
import requests
import configparser
import xml.etree.ElementTree as ET
from urllib.parse import quote
import time
from collections import defaultdict
from plexapi.server import PlexServer
import logging
import json
from datetime import datetime
import schedule
import discord
from discord import Webhook, Embed, Color
import asyncio
import aiohttp
import subprocess # <- ADDED: For running the FFprobe command

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

class RunStats:
    def __init__(self):
        self.start_time = datetime.now()
        self.missing_items = defaultdict(list)
        self.errors = []
        self.warnings = []
        self.total_scanned = 0
        self.total_missing = 0
        self.broken_symlinks = 0

    def add_missing_item(self, library_name, file_path):
        self.missing_items[library_name].append(file_path)
        self.total_missing += 1

    def add_error(self, error):
        self.errors.append(error)

    def add_warning(self, warning):
        self.warnings.append(warning)

    def increment_scanned(self):
        self.total_scanned += 1

    def increment_broken_symlinks(self):
        self.broken_symlinks += 1

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
            # Create webhook client with aiohttp session
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(DISCORD_WEBHOOK_URL, session=session)

                # Create embed
                embed = Embed(
                    title="Rescan Summary",
                    color=Color.blue(),
                    timestamp=datetime.now()
                )

                # Add overview
                embed.add_field(
                    name="📊 Overview",
                    value=f"Found **{self.total_missing}** items from **{self.total_scanned}** scanned files",
                    inline=False
                )

                # Add broken symlinks summary if any
                if self.broken_symlinks > 0:
                    embed.add_field(
                        name="⚠️ Issues",
                        value=f"Broken Symlinks Skipped: **{self.broken_symlinks}**",
                        inline=False
                    )

                # Add library-specific stats
                for library, items in self.missing_items.items():
                    embed.add_field(
                        name=f"📁 {library}",
                        value=f"Found: **{len(items)}** items",
                        inline=True
                    )

                # Add other errors and warnings if any
                if self.errors or self.warnings:
                    error_text = "\n".join([f"❌ {e}" for e in self.errors])
                    warning_text = "\n".join([f"⚠️ {w}" for w in self.warnings])
                    if error_text or warning_text:
                        embed.add_field(
                            name="⚠️ Other Issues",
                            value=f"{error_text}\n{warning_text}",
                            inline=False
                        )

                # Add footer
                embed.set_footer(text=f"Run Time: {self.get_run_time()}")

                # Send webhook
                await send_discord_webhook(webhook, embed)
                logger.info("✅ Discord notification sent successfully")

        except discord.HTTPException as e:
            logger.error(f"Discord API error: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {str(e)}")

async def send_discord_webhook(webhook, embed):
    """Send a Discord webhook message."""
    try:
        # Check if embed exceeds Discord's limits
        if len(str(embed)) > 6000:
            # Split into multiple embeds
            base_embed = Embed(
                title=embed.title,
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            # Add overview field
            if embed.fields and embed.fields[0].name == "📊 Overview":
                base_embed.add_field(
                    name=embed.fields[0].name,
                    value=embed.fields[0].value,
                    inline=False
                )
            
            # Send base embed
            await webhook.send(
                embed=base_embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True
            )
            
            # Create additional embeds for libraries
            current_embed = Embed(
                title="📁 Library Details",
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            # Add library fields
            for field in embed.fields[1:]:
                if field.name.startswith("📁"):
                    if len(str(current_embed)) + len(str(field)) > 6000:
                        # Send current embed and create new one
                        await webhook.send(
                            embed=current_embed,
                            avatar_url=DISCORD_AVATAR_URL,
                            username=DISCORD_WEBHOOK_NAME,
                            wait=True
                        )
                        current_embed = Embed(
                            title="📁 Library Details (continued)",
                            color=embed.color,
                            timestamp=embed.timestamp
                        )
                    current_embed.add_field(
                        name=field.name,
                        value=field.value,
                        inline=field.inline
                    )
            
            # Send final library embed if it has fields
            if current_embed.fields:
                await webhook.send(
                    embed=current_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True
                )
            
            # Send issues in separate embed if they exist
            if embed.fields and embed.fields[-1].name == "⚠️ Issues":
                issues_embed = Embed(
                    title="⚠️ Issues",
                    color=Color.red(),
                    timestamp=embed.timestamp
                )
                issues_embed.add_field(
                    name=embed.fields[-1].name,
                    value=embed.fields[-1].value,
                    inline=False
                )
                await webhook.send(
                    embed=issues_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True
                )
        else:
            # Send single embed if within limits
            await webhook.send(
                embed=embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True
            )
    except discord.HTTPException as e:
        logger.error(f"Discord API error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to send webhook: {str(e)}")
        raise

def get_library_ids():
    """Fetch library section IDs and paths dynamically from Plex."""
    global library_ids, library_paths
    for section in plex.library.sections():
        lib_type = section.type
        lib_key = section.key
        lib_title = section.title
        library_ids[lib_type] = lib_key
        
        # Get the path for this library
        for location in section.locations:
            library_paths[location] = lib_key
            logger.debug(f"Found library '{lib_title}' (ID: {lib_key}) at path: {location}")

    return library_ids

def get_library_id_for_path(file_path):
    """Get the library section ID for a given file path."""
    # Get all library sections
    url = f"{PLEX_URL}/library/sections"
    params = {'X-Plex-Token': TOKEN}
    response = requests.get(url, params=params)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    
    # Find matching sections
    matching_sections = []
    for section in root.findall('Directory'):
        section_type = section.get('type')
        section_id = section.get('key')
        section_title = section.get('title')
        
        # Get all locations for this section
        for location in section.findall('Location'):
            location_path = location.get('path')
            matching_sections.append((section_id, section_type, location_path, section_title))
    
    # Find best matching section (prefer more specific matches)
    best_match = None
    best_match_length = 0
    
    for section_id, section_type, location_path, section_title in matching_sections:
        # Normalize paths for comparison
        normalized_scan_path = os.path.normpath(file_path)
        normalized_location = os.path.normpath(location_path)
        
        # Check if the file path starts with the library location
        if normalized_scan_path.startswith(normalized_location):
            # Use the longest matching path (most specific)
            if len(normalized_location) > best_match_length:
                best_match = (section_id, section_title)
                best_match_length = len(normalized_location)
    
    if best_match:
        section_id, section_title = best_match
        logger.debug(f"Found best match in section: {section_title} (id: {section_id})")
        return section_id, section_title
    
    logger.warning(f"No matching library found for path: {file_path}")
    return None, None

def cache_library_files(library_id):
    """Cache all files in a library section."""
    if library_id in library_files:
        logger.debug(f"Using cached files for library {BOLD}{library_id}{RESET}...")
        return  # Already cached
    
    try:
        section = plex.library.sectionByID(int(library_id))
        logger.info(f"💾 Initializing cache for library {BOLD}{section.title}{RESET}...")
        cache_start = time.time()
        
        if section.type == 'show':
            # For TV shows, get all episodes
            for show in section.all():
                for episode in show.episodes():
                    for media in episode.media:
                        for part in media.parts:
                            if part.file:
                                library_files[library_id].add(part.file)
        else:
            # For movies, get all items
            for item in section.all():
                for media in item.media:
                    for part in media.parts:
                        if part.file:
                            library_files[library_id].add(part.file)
        
        cache_time = time.time() - cache_start
        logger.info(f"💾 Cache initialized for library {BOLD}{section.title}{RESET}: {BOLD}{len(library_files[library_id])}{RESET} files in {BOLD}{cache_time:.2f}{RESET} seconds")
    except Exception as e:
        logger.error(f"Error caching library {library_id}: {str(e)}")
        # Clear the cache for this library if there was an error
        if library_id in library_files:
            del library_files[library_id]

def is_in_plex(file_path):
    """Check if a file exists in Plex by searching in the appropriate library section."""
    # Get the library ID for this path
    library_id, library_title = get_library_id_for_path(file_path)
    if not library_id:
        return False

    # Cache library files if not already cached
    cache_library_files(library_id)
    
    # Check if file exists in cached paths using exact matching
    is_found = file_path in library_files[library_id]
    if is_found:
        logger.debug(f"Found in cache: {BOLD}{file_path}{RESET}")
    return is_found

def scan_folder(library_id, folder_path):
    """Trigger a library scan for a specific folder."""
    # Ensure library_id is a string
    library_id = str(library_id)
    encoded_path = quote(folder_path)
    url = f"{PLEX_URL}/library/sections/{library_id}/refresh?path={encoded_path}&X-Plex-Token={TOKEN}"
    logger.debug(f"Scan URL: {url}")
    response = requests.get(url)
    logger.info(f"🔎 Scan triggered for: {BOLD}{folder_path}{RESET}")
    logger.info(f"⏳ Waiting {BOLD}{SCAN_INTERVAL}{RESET} seconds before next scan")
    time.sleep(SCAN_INTERVAL)  # Wait between scans

# --- MODIFIED FUNCTION ---
def is_broken_symlink(file_path):
    """
    Check if a file is a symlink and confirm it's broken using both
    os.path checks and an FFprobe read attempt.
    """
    if not os.path.islink(file_path):
        return False
    
    # Fast Check 1: Does the target path exist? (Original method)
    target_exists = os.path.exists(os.path.realpath(file_path))
    
    if not target_exists:
        # It's a symlink, and its target is missing -> It's broken.
        logger.debug(f"Fast check failed (target missing): {file_path}")
        return True
    
    # Slow Confirmation Check: Only run if target exists, to check file validity.
    logger.debug(f"🔍 Confirming symlink target validity with FFprobe: {file_path}")
    
    # FFprobe command to read the format metadata of the file
    command = [
        'ffprobe',
        '-v', 'error',          # Suppress detailed output, only show errors
        '-show_entries', 'format=duration', # Only ask for duration metadata
        '-of', 'default=noprint_wrappers=1:nokey=1', # Simple output format
        file_path
    ]
    
    try:
        # Run the command and capture output/errors
        result = subprocess.run(
            command,
            check=False,        # Do not raise an exception on non-zero exit code
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # If the return code is non-zero, FFprobe failed to read the file
        if result.returncode != 0:
            logger.warning(f"❌ FFprobe failed to read file (Corrupt/Unreadable): {file_path}")
            # If the target exists but FFprobe fails, we treat it as functionally broken media
            return True 
            
    except FileNotFoundError:
        # This handles the case where 'ffprobe' is not installed or not in PATH
        # Since the fast check passed (target_exists is True), we assume it's good
        logger.error("FFprobe not found. Cannot perform media validity check. Check skipped.")
        return False 
    except Exception as e:
        logger.error(f"Error running FFprobe on {file_path}: {str(e)}")
        return True # Assume broken on unexpected error

    return False # Symlink target exists and FFprobe successfully read it
# ---------------------------

def run_scan():
    """Main scan logic."""
    stats = RunStats()
    
    # Clear any existing cache at the start of a new scan
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
                    continue  # skip hidden/system files

                file_ext = os.path.splitext(file)[1].lower()
                if file_ext not in MEDIA_EXTENSIONS:
                    continue  # skip non-media files

                file_path = os.path.join(root, file)
                
                # Check for broken symlinks if enabled
                if SYMLINK_CHECK and is_broken_symlink(file_path):
                    warning_msg = f"⏩ Skipping broken symlink/invalid media: {file_path}"
                    logger.warning(warning_msg)
                    stats.increment_broken_symlinks()
                    continue

                stats.increment_scanned()

                if not is_in_plex(file_path):
                    library_id, library_title = get_library_id_for_path(file_path)
                    if library_title:
                        stats.add_missing_item(library_title, file_path)
                        logger.info(f"📁 Found missing item: {BOLD}{file_path}{RESET}")
                    
                        # Determine library type and scan parent folder
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

def main():
    """Main function to run the scanner on a schedule."""
    logger.info("Starting Plex Missing Files Scanner")
    logger.info(f"Will run every {BOLD}{RUN_INTERVAL}{RESET} hours")
    
    # Run immediately on startup
    run_scan()
    
    # Schedule subsequent runs
    schedule.every(RUN_INTERVAL).hours.do(run_scan)
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute for pending tasks

if __name__ == '__main__':
    # Check if config exists
    if not os.path.exists('config.ini'):
        logger.error("❌ config.ini not found. Please copy config-example.ini to config.ini and configure it.")
        exit(1)
    
    main()