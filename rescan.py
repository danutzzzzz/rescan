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
import urllib.request
import urllib.error

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
DELETE_BROKEN = config.getboolean('behaviour', 'delete_broken', fallback=False)
NOTIFICATIONS_ENABLED = config.getboolean('notifications', 'enabled', fallback=True)

directories_raw = config['scan']['directories']
SCAN_PATHS = [path.strip() for path in directories_raw.replace('\n', ',').split(',') if path.strip()]

# Media file extensions to look for
MEDIA_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.m4v', '.m4p', '.m4b', '.m4r', '.3gp', '.mpg', '.mpeg',
    '.m2v', '.m2ts', '.ts', '.vob', '.iso'
}

library_ids = {}
library_paths = {}
library_files = defaultdict(set)  # Cache of files in each library

plex = None

BOLD = '\033[1m'
RESET = '\033[0m'
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'

VALID_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
level_name = LOG_LEVEL.upper()
if level_name not in VALID_LEVELS:
    level_name = "INFO"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%d %b %Y | %I:%M:%S %p'
)
logger = logging.getLogger(__name__)

def get_plex_server():
    """Initialize and return Plex server connection."""
    global plex
    if plex is None:
        try:
            logger.info(f"🔌 Connecting to Plex server at {PLEX_URL}...")
            plex = PlexServer(PLEX_URL, TOKEN)
            logger.info(f"✅ Connected to Plex server: {plex.friendlyName}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Plex server: {e}")
            raise
    return plex

def load_arr_instances():
    """Load Sonarr and Radarr instances from config.ini"""
    sonarr_instances = []
    radarr_instances = []
    
    if config.has_section('sonarr'):
        for key in config['sonarr']:
            if key.startswith('instance'):
                try:
                    instance_data = json.loads(config['sonarr'][key])
                    sonarr_instances.append(instance_data)
                    logger.info(f"✅ Loaded Sonarr instance: {BOLD}{instance_data.get('name', 'Unknown')}{RESET}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to parse Sonarr {key}: {e}")
                except Exception as e:
                    logger.error(f"❌ Error loading Sonarr {key}: {e}")
    
    if config.has_section('radarr'):
        for key in config['radarr']:
            if key.startswith('instance'):
                try:
                    instance_data = json.loads(config['radarr'][key])
                    radarr_instances.append(instance_data)
                    logger.info(f"✅ Loaded Radarr instance: {BOLD}{instance_data.get('name', 'Unknown')}{RESET}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to parse Radarr {key}: {e}")
                except Exception as e:
                    logger.error(f"❌ Error loading Radarr {key}: {e}")
    
    return sonarr_instances, radarr_instances

SONARR_INSTANCES, RADARR_INSTANCES = load_arr_instances()

def api_request(url, api_key, endpoint, method="GET", body=None):
    """Make API request to Sonarr/Radarr."""
    full_url = f"{url.rstrip('/')}/api/v3/{endpoint}"
    headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}
    data = json.dumps(body).encode('utf-8') if body else None
    
    try:
        req = urllib.request.Request(full_url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as response:
            resp_text = response.read().decode()
            if not resp_text: 
                return {}  # Handle empty 200 OK responses
            return json.loads(resp_text)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            logger.error(f"{RED}API Error {e.code}: {e.read().decode()}{RESET}")
        return None
    except Exception as e:
        logger.error(f"{RED}Request Error: {e}{RESET}")
        return None

def map_path_to_remote(local_path, local_prefix, remote_prefix):
    """Map local path to remote path for Sonarr/Radarr."""
    if local_path.startswith(local_prefix):
        return local_path.replace(local_prefix, remote_prefix, 1)
    return local_path

def map_remote_to_local(remote_path, local_prefix, remote_prefix):
    """Map remote path to local path."""
    if remote_path.startswith(remote_prefix):
        return remote_path.replace(remote_prefix, local_prefix, 1)
    return None

def blocklist_radarr(url, api_key, movie_id):
    """Blocklist a movie release in Radarr."""
    history = api_request(url, api_key, f"history/movie?movieId={movie_id}")
    
    if history and len(history) > 0:
        grabs = [h for h in history if h.get('eventType') == 'grabbed']
        
        if grabs:
            grabs.sort(key=lambda x: x['date'], reverse=True)
            recent_grab = grabs[0]
            
            logger.info(f"{YELLOW}Found history item: {recent_grab.get('sourceTitle', 'Unknown')}{RESET}")
            logger.info(f"{YELLOW}Marking as Failed (Blocklisting)...{RESET}")
            
            api_request(url, api_key, f"history/failed/{recent_grab['id']}", method="POST")
            return True
    
    logger.warning(f"{RED}No grab history found to blocklist.{RESET}")
    return False

def blocklist_sonarr(url, api_key, episode_id):
    """Blocklist an episode release in Sonarr."""
    history = api_request(url, api_key, f"history?episodeId={episode_id}")
    
    records = history.get('records', []) if history else []
    
    if records:
        grabs = [r for r in records if r.get('eventType') == 'grabbed']
        
        if grabs:
            grabs.sort(key=lambda k: k['date'], reverse=True)
            recent_grab = grabs[0]
            
            logger.info(f"{YELLOW}Found history item: {recent_grab.get('sourceTitle', 'Unknown')}{RESET}")
            logger.info(f"{YELLOW}Marking as Failed (Blocklisting)...{RESET}")
            
            api_request(url, api_key, f"history/failed/{recent_grab['id']}", method="POST")
            return True
        
    logger.warning(f"{RED}No grab history found for this episode.{RESET}")
    return False

def find_radarr_instance(filepath):
    """Find which Radarr instance manages this file."""
    for instance in RADARR_INSTANCES:
        local_prefix = instance.get('local_path_prefix', '')
        if filepath.startswith(local_prefix):
            return instance
    return None

def find_sonarr_instance(filepath):
    """Find which Sonarr instance manages this file."""
    for instance in SONARR_INSTANCES:
        local_prefix = instance.get('local_path_prefix', '')
        if filepath.startswith(local_prefix):
            return instance
    return None

def trigger_radarr_fix(filepath, instance):
    """Blocklist and search for replacement in Radarr."""
    url = instance['url']
    api_key = instance['api_key']
    local_prefix = instance.get('local_path_prefix', '')
    remote_prefix = instance.get('remote_path_prefix', '')
    
    remote_path = map_path_to_remote(filepath, local_prefix, remote_prefix)
    logger.info(f"{YELLOW}Processing Radarr fix for: {remote_path}{RESET}")

    movies = api_request(url, api_key, "movie")
    if not movies: 
        return False
    
    target_movie = next((m for m in movies if m['path'] in remote_path), None)
    
    if target_movie:
        # 1. Blocklist
        blocklist_radarr(url, api_key, target_movie['id'])

        # 2. Delete File
        files = api_request(url, api_key, f"moviefile?movieId={target_movie['id']}")
        if files:
            target_file = next((f for f in files if f['path'] == remote_path), None)

            if target_file:
                logger.info(f"{YELLOW}Deleting file ID {target_file['id']}...{RESET}")
                api_request(url, api_key, f"moviefile/{target_file['id']}", method="DELETE")

        # 3. Search
        logger.info(f"{GREEN}Triggering Search for: {target_movie['title']}{RESET}")
        api_request(url, api_key, "command", method="POST", 
                   body={"name": "MoviesSearch", "movieIds": [target_movie['id']]})
        return True
    
    return False

def trigger_sonarr_fix(filepath, instance):
    """Blocklist and search for replacement in Sonarr."""
    url = instance['url']
    api_key = instance['api_key']
    local_prefix = instance.get('local_path_prefix', '')
    remote_prefix = instance.get('remote_path_prefix', '')
    
    remote_path = map_path_to_remote(filepath, local_prefix, remote_prefix)
    logger.info(f"{YELLOW}Processing Sonarr fix for: {remote_path}{RESET}")

    series_list = api_request(url, api_key, "series")
    if not series_list: 
        return False
    
    target_series = next((s for s in series_list if s['path'] in remote_path), None)
            
    if not target_series:
        logger.warning(f"{RED}Series not found in Sonarr DB.{RESET}")
        return False

    # Find the file
    files = api_request(url, api_key, f"episodefile?seriesId={target_series['id']}")
    if not files:
        return False
        
    target_file = next((f for f in files if f['path'] == remote_path), None)

    if target_file:
        file_id = target_file['id']
        
        # Find which Episode owns this file
        all_episodes = api_request(url, api_key, f"episode?seriesId={target_series['id']}")
        linked_episodes = [ep for ep in all_episodes if ep.get('episodeFileId') == file_id]
        
        if linked_episodes:
            target_ep = linked_episodes[0]
            logger.info(f"{GREEN}File belongs to: S{target_ep['seasonNumber']}E{target_ep['episodeNumber']} - {target_ep['title']}{RESET}")
            
            # 1. Blocklist via Episode ID
            blocklist_sonarr(url, api_key, target_ep['id'])
            
            # 2. Delete File
            logger.info(f"{YELLOW}Deleting file ID {file_id}...{RESET}")
            api_request(url, api_key, f"episodefile/{file_id}", method="DELETE")
            
            # 3. Rescan Series
            logger.info(f"{YELLOW}Rescanning Series...{RESET}")
            api_request(url, api_key, "command", method="POST", 
                       body={"name": "RescanSeries", "seriesId": target_series['id']})
            
            # 4. Search Missing Episode
            missing_ep_ids = [ep['id'] for ep in linked_episodes]
            logger.info(f"{GREEN}Triggering Search for missing episode(s)...{RESET}")
            api_request(url, api_key, "command", method="POST", 
                       body={"name": "EpisodeSearch", "episodeIds": missing_ep_ids})
            return True
            
        else:
            logger.warning(f"{RED}Found file in DB, but no episodes are linked? Deleting orphan.{RESET}")
            api_request(url, api_key, f"episodefile/{file_id}", method="DELETE")
            return True
    
    return False

# ==========================================
#      ORIGINAL RESCAN.PY FUNCTIONS
# ==========================================

class RunStats:
    def __init__(self):
        self.start_time = datetime.now()
        self.missing_items = defaultdict(list)
        self.errors = []
        self.warnings = []
        self.total_scanned = 0
        self.total_missing = 0
        self.broken_symlinks = 0
        self.blocklisted_items = 0

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
    
    def increment_blocklisted(self):
        self.blocklisted_items += 1

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

                if self.broken_symlinks > 0 or self.blocklisted_items > 0:
                    issues_text = ""
                    if self.broken_symlinks > 0:
                        issues_text += f"Broken Symlinks Removed: **{self.broken_symlinks}**\n"
                    if self.blocklisted_items > 0:
                        issues_text += f"Items Blocklisted & Redownloading: **{self.blocklisted_items}**"
                    
                    embed.add_field(
                        name="⚠️ Issues",
                        value=issues_text,
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
                    if error_text or warning_text:
                        embed.add_field(
                            name="⚠️ Other Issues",
                            value=f"{error_text}\n{warning_text}",
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
    """Send a Discord webhook message."""
    try:
        if len(str(embed)) > 6000:
            base_embed = Embed(
                title=embed.title,
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            if embed.fields and embed.fields[0].name == "📊 Overview":
                base_embed.add_field(
                    name=embed.fields[0].name,
                    value=embed.fields[0].value,
                    inline=False
                )
            
            await webhook.send(
                embed=base_embed,
                avatar_url=DISCORD_AVATAR_URL,
                username=DISCORD_WEBHOOK_NAME,
                wait=True
            )
            
            current_embed = Embed(
                title="📁 Library Details",
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            for field in embed.fields[1:]:
                if field.name.startswith("📁"):
                    if len(str(current_embed)) + len(str(field)) > 6000:
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
            
            if current_embed.fields:
                await webhook.send(
                    embed=current_embed,
                    avatar_url=DISCORD_AVATAR_URL,
                    username=DISCORD_WEBHOOK_NAME,
                    wait=True
                )
            
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
    plex = get_plex_server()
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
    """Get the library section ID for a given file path."""
    url = f"{PLEX_URL}/library/sections"
    params = {'X-Plex-Token': TOKEN}
    response = requests.get(url, params=params)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    
    matching_sections = []
    for section in root.findall('Directory'):
        section_type = section.get('type')
        section_id = section.get('key')
        section_title = section.get('title')
        
        for location in section.findall('Location'):
            location_path = location.get('path')
            matching_sections.append((section_id, section_type, location_path, section_title))
    
    best_match = None
    best_match_length = 0
    
    for section_id, section_type, location_path, section_title in matching_sections:
        normalized_scan_path = os.path.normpath(file_path)
        normalized_location = os.path.normpath(location_path)
        
        if normalized_scan_path.startswith(normalized_location):
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
        return
    
    try:
        plex = get_plex_server()
        section = plex.library.sectionByID(int(library_id))
        logger.info(f"💾 Initializing cache for library {BOLD}{section.title}{RESET}...")
        cache_start = time.time()
        
        if section.type == 'show':
            for show in section.all():
                for episode in show.episodes():
                    for media in episode.media:
                        for part in media.parts:
                            if part.file:
                                library_files[library_id].add(part.file)
        else:
            for item in section.all():
                for media in item.media:
                    for part in media.parts:
                        if part.file:
                            library_files[library_id].add(part.file)
        
        cache_time = time.time() - cache_start
        logger.info(f"💾 Cache initialized for library {BOLD}{section.title}{RESET}: {BOLD}{len(library_files[library_id])}{RESET} files in {BOLD}{cache_time:.2f}{RESET} seconds")
    except Exception as e:
        logger.error(f"Error caching library {library_id}: {str(e)}")
        if library_id in library_files:
            del library_files[library_id]

def is_in_plex(file_path):
    """Check if a file exists in Plex by searching in the appropriate library section."""
    library_id, library_title = get_library_id_for_path(file_path)
    if not library_id:
        return False

    cache_library_files(library_id)
    
    is_found = file_path in library_files[library_id]
    if is_found:
        logger.debug(f"Found in cache: {BOLD}{file_path}{RESET}")
    return is_found

def scan_folder(library_id, folder_path):
    """Trigger a library scan for a specific folder."""
    library_id = str(library_id)
    encoded_path = quote(folder_path)
    url = f"{PLEX_URL}/library/sections/{library_id}/refresh?path={encoded_path}&X-Plex-Token={TOKEN}"
    logger.debug(f"Scan URL: {url}")
    response = requests.get(url)
    logger.info(f"🔎 Scan triggered for: {BOLD}{folder_path}{RESET}")
    logger.info(f"⏳ Waiting {BOLD}{SCAN_INTERVAL}{RESET} seconds before next scan")
    time.sleep(SCAN_INTERVAL)

def is_broken_symlink(file_path):
    """Check if a file is a broken symlink."""
    if not os.path.islink(file_path):
        return False
    return not os.path.exists(os.path.realpath(file_path))

def handle_broken_symlink(file_path, stats):
    """Handle broken symlink with optional Sonarr/Radarr integration."""
    logger.warning(f"🔗 Found broken symlink: {file_path}")
    
    # Try Radarr first
    radarr_instance = find_radarr_instance(file_path)
    if radarr_instance:
        logger.info(f"{YELLOW}Attempting Radarr fix with instance: {radarr_instance.get('name', 'Unknown')}{RESET}")
        if trigger_radarr_fix(file_path, radarr_instance):
            stats.increment_blocklisted()
            stats.increment_broken_symlinks()
            return True
    
    # Try Sonarr if Radarr didn't handle it
    sonarr_instance = find_sonarr_instance(file_path)
    if sonarr_instance:
        logger.info(f"{YELLOW}Attempting Sonarr fix with instance: {sonarr_instance.get('name', 'Unknown')}{RESET}")
        if trigger_sonarr_fix(file_path, sonarr_instance):
            stats.increment_blocklisted()
            stats.increment_broken_symlinks()
            return True
    
    # Fallback to simple delete if no instance found
    if DELETE_BROKEN:
        try:
            os.remove(file_path)
            logger.warning(f"🗑️ Deleted broken symlink: {file_path}")
            stats.increment_broken_symlinks()
            return True
        except Exception as e:
            logger.error(f"❌ Failed to delete symlink {file_path}: {e}")
            return False
    else:
        logger.warning(f"⏩ Skipping broken symlink: {file_path}")
        stats.increment_broken_symlinks()
        return False

def run_scan():
    """Main scan logic."""
    stats = RunStats()
    
    # Test Plex connection first
    try:
        get_plex_server()
    except Exception as e:
        error_msg = f"Cannot connect to Plex server: {e}"
        logger.error(error_msg)
        stats.add_error(error_msg)
        asyncio.run(stats.send_discord_summary())
        return
    
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
                
                # Check for broken symlinks if enabled
                if SYMLINK_CHECK and is_broken_symlink(file_path):
                    handle_broken_symlink(file_path, stats)
                    continue

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

    asyncio.run(stats.send_discord_summary())

def main():
    """Main function to run the scanner on a schedule."""
    logger.info("🚀 Starting Plex Missing Files Scanner with Sonarr/Radarr Integration")
    logger.info(f"⏱️ Will run every {BOLD}{RUN_INTERVAL}{RESET} hours")
    
    # Log loaded instances
    if RADARR_INSTANCES:
        logger.info(f"🎬 Loaded {BOLD}{len(RADARR_INSTANCES)}{RESET} Radarr instance(s)")
    else:
        logger.warning(f"{YELLOW}⚠️  No Radarr instances configured{RESET}")
        
    if SONARR_INSTANCES:
        logger.info(f"📺 Loaded {BOLD}{len(SONARR_INSTANCES)}{RESET} Sonarr instance(s)")
    else:
        logger.warning(f"{YELLOW}⚠️  No Sonarr instances configured{RESET}")
    
    if not RADARR_INSTANCES and not SONARR_INSTANCES:
        logger.warning(f"{YELLOW}⚠️  Sonarr/Radarr integration disabled - no instances configured{RESET}")
    
    logger.info("")
    
    run_scan()
    
    schedule.every(RUN_INTERVAL).hours.do(run_scan)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    if not os.path.exists('config.ini'):
        logger.error("❌ config.ini not found. Please copy config-example.ini to config.ini and configure it.")
        exit(1)
    
    main()