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

PLEX_URL = config['plex']['server'].strip()
TOKEN = config['plex']['token'].strip()
LOG_LEVEL = config['logs']['loglevel'].strip().split()[0]
SCAN_INTERVAL = int(config['behaviour']['scan_interval'])
RUN_INTERVAL = int(config['behaviour']['run_interval'])
DISCORD_WEBHOOK_URL = config['notifications']['discord_webhook_url'].strip()
DISCORD_AVATAR_URL = "https://raw.githubusercontent.com/pukabyte/rescan/master/assets/logo.png"
DISCORD_WEBHOOK_NAME = "Rescan"
SYMLINK_CHECK = config.getboolean('behaviour', 'symlink_check', fallback=False)
DELETE_BROKEN = config.getboolean('behaviour', 'delete_broken', fallback=False)
ENABLE_BLOCKLIST = config.getboolean('behaviour', 'enable_blocklist', fallback=True)
NOTIFICATIONS_ENABLED = config.getboolean('notifications', 'enabled', fallback=True)

directories_raw = config['scan']['directories']
SCAN_PATHS = [path.strip() for path in directories_raw.replace('\n', ',').split(',') if path.strip()]

MEDIA_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.m4v', '.m4p', '.m4b', '.m4r', '.3gp', '.mpg', '.mpeg',
    '.m2v', '.m2ts', '.ts', '.vob', '.iso'
}

library_ids = {}
library_paths = {}
library_files = defaultdict(set)
plex = None

BOLD = '\033[1m'
RESET = '\033[0m'
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'

valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LOG_LEVEL.upper() not in valid_log_levels:
    print(f"Warning: Invalid log level '{LOG_LEVEL}', defaulting to INFO")
    LOG_LEVEL = 'INFO'

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%d %b %Y | %I:%M:%S %p'
)
logger = logging.getLogger(__name__)

def get_plex_server():
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
    sonarr_instances = []
    radarr_instances = []
    
    if config.has_section('sonarr'):
        for key in config['sonarr']:
            if key.startswith('instance'):
                try:
                    instance_data = json.loads(config['sonarr'][key])
                    sonarr_instances.append(instance_data)
                    instance_name = instance_data.get('name', 'Unknown')
                    logger.info(f"✅ Loaded Sonarr instance: {BOLD}{instance_name}{RESET}")
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
                    instance_name = instance_data.get('name', 'Unknown')
                    logger.info(f"✅ Loaded Radarr instance: {BOLD}{instance_name}{RESET}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to parse Radarr {key}: {e}")
                except Exception as e:
                    logger.error(f"❌ Error loading Radarr {key}: {e}")
    
    return sonarr_instances, radarr_instances

SONARR_INSTANCES, RADARR_INSTANCES = load_arr_instances()

def api_request(url, api_key, endpoint, method="GET", body=None):
    full_url = f"{url.rstrip('/')}/api/v3/{endpoint}"
    headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}
    data = json.dumps(body).encode('utf-8') if body else None
    
    try:
        req = urllib.request.Request(full_url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as response:
            resp_text = response.read().decode()
            if not resp_text: 
                return {}
            return json.loads(resp_text)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            logger.error(f"{RED}API Error {e.code}: {e.read().decode()}{RESET}")
        return None
    except Exception as e:
        logger.error(f"{RED}Request Error: {e}{RESET}")
        return None

def map_path_to_remote(local_path, local_prefix, remote_prefix):
    if local_path.startswith(local_prefix):
        return local_path.replace(local_prefix, remote_prefix, 1)
    return local_path

def blocklist_radarr(url, api_key, movie_id):
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
    for instance in RADARR_INSTANCES:
        local_prefix = instance.get('local_path_prefix', '')
        if filepath.startswith(local_prefix):
            return instance
    return None

def find_sonarr_instance(filepath):
    for instance in SONARR_INSTANCES:
        local_prefix = instance.get('local_path_prefix', '')
        if filepath.startswith(local_prefix):
            return instance
    return None

def trigger_radarr_fix(filepath, instance, do_blocklist=True):
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
        if do_blocklist:
            blocklist_radarr(url, api_key, target_movie['id'])
        else:
            logger.info(f"{YELLOW}Skipping blocklist (disabled in config){RESET}")

        files = api_request(url, api_key, f"moviefile?movieId={target_movie['id']}")
        if files:
            target_file = next((f for f in files if f['path'] == remote_path), None)
            if target_file:
                logger.info(f"{YELLOW}Deleting file ID {target_file['id']}...{RESET}")
                api_request(url, api_key, f"moviefile/{target_file['id']}", method="DELETE")

        logger.info(f"{GREEN}Triggering Search for: {target_movie['title']}{RESET}")
        api_request(url, api_key, "command", method="POST", 
                   body={"name": "MoviesSearch", "movieIds": [target_movie['id']]})
        return True
    
    return False

def trigger_sonarr_fix(filepath, instance, do_blocklist=True):
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

    files = api_request(url, api_key, f"episodefile?seriesId={target_series['id']}")
    if not files:
        return False
        
    target_file = next((f for f in files if f['path'] == remote_path), None)

    if target_file:
        file_id = target_file['id']
        
        all_episodes = api_request(url, api_key, f"episode?seriesId={target_series['id']}")
        linked_episodes = [ep for ep in all_episodes if ep.get('episodeFileId') == file_id]
        
        if linked_episodes:
            target_ep = linked_episodes[0]
            logger.info(f"{GREEN}File belongs to: S{target_ep['seasonNumber']}E{target_ep['episodeNumber']} - {target_ep['title']}{RESET}")
            
            if do_blocklist:
                blocklist_sonarr(url, api_key, target_ep['id'])
            else:
                logger.info(f"{YELLOW}Skipping blocklist (disabled in config){RESET}")
            
            logger.info(f"{YELLOW}Deleting file ID {file_id}...{RESET}")
            api_request(url, api_key, f"episodefile/{file_id}", method="DELETE")
            
            logger.info(f"{YELLOW}Rescanning Series...{RESET}")
            api_request(url, api_key, "command", method="POST", 
                       body={"name": "RescanSeries", "seriesId": target_series['id']})
            
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

def handle_broken_symlink(file_path, stats):
    logger.warning(f"🔗 Found broken symlink: {file_path}")
    
    if RADARR_INSTANCES or SONARR_INSTANCES:
        radarr_instance = find_radarr_instance(file_path)
        if radarr_instance:
            logger.info(f"{YELLOW}Attempting Radarr fix with instance: {radarr_instance.get('name', 'Unknown')}{RESET}")
            if trigger_radarr_fix(file_path, radarr_instance, do_blocklist=ENABLE_BLOCKLIST):
                if ENABLE_BLOCKLIST:
                    stats.increment_blocklisted()
                stats.increment_broken_symlinks()
                return True
        
        sonarr_instance = find_sonarr_instance(file_path)
        if sonarr_instance:
            logger.info(f"{YELLOW}Attempting Sonarr fix with instance: {sonarr_instance.get('name', 'Unknown')}{RESET}")
            if trigger_sonarr_fix(file_path, sonarr_instance, do_blocklist=ENABLE_BLOCKLIST):
                if ENABLE_BLOCKLIST:
                    stats.increment_blocklisted()
                stats.increment_broken_symlinks()
                return True
    
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
    try:
        await webhook.send(embed=embed, avatar_url=DISCORD_AVATAR_URL, username=DISCORD_WEBHOOK_NAME, wait=True)
    except Exception as e:
        logger.error(f"Failed to send webhook: {str(e)}")
        raise

def get_library_ids():
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
            matching_sections.append((section_id, location_path, section_title))
    
    best_match = None
    best_match_length = 0
    
    for section_id, location_path, section_title in matching_sections:
        normalized_scan_path = os.path.normpath(file_path)
        normalized_location = os.path.normpath(location_path)
        
        if normalized_scan_path.startswith(normalized_location):
            if len(normalized_location) > best_match_length:
                best_match = (section_id, section_title)
                best_match_length = len(normalized_location)
    
    if best_match:
        return best_match
    
    return None, None

def cache_library_files(library_id):
    if library_id in library_files:
        return
    
    try:
        plex = get_plex_server()
        section = plex.library.sectionByID(int(library_id))
        logger.info(f"💾 Initializing cache for library {BOLD}{section.title}{RESET}...")
        
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
        
        logger.info(f"💾 Cache initialized: {BOLD}{len(library_files[library_id])}{RESET} files")
    except Exception as e:
        logger.error(f"Error caching library {library_id}: {str(e)}")

def is_in_plex(file_path):
    library_id, library_title = get_library_id_for_path(file_path)
    if not library_id:
        return False

    cache_library_files(library_id)
    return file_path in library_files[library_id]

def scan_folder(library_id, folder_path):
    library_id = str(library_id)
    encoded_path = quote(folder_path)
    url = f"{PLEX_URL}/library/sections/{library_id}/refresh?path={encoded_path}&X-Plex-Token={TOKEN}"
    requests.get(url)
    logger.info(f"🔎 Scan triggered for: {BOLD}{folder_path}{RESET}")
    time.sleep(SCAN_INTERVAL)

def is_broken_symlink(file_path):
    if not os.path.islink(file_path):
        return False
    return not os.path.exists(os.path.realpath(file_path))

def run_scan():
    stats = RunStats()
    
    try:
        get_plex_server()
    except Exception as e:
        stats.add_error(f"Cannot connect to Plex: {e}")
        asyncio.run(stats.send_discord_summary())
        return
    
    library_files.clear()
    library_ids_map = get_library_ids()

    scanned_folders = set()

    for SCAN_PATH in SCAN_PATHS:
        logger.info(f"\nScanning: {BOLD}{SCAN_PATH}{RESET}")

        if not os.path.isdir(SCAN_PATH):
            stats.add_error(f"Directory not found: {SCAN_PATH}")
            continue

        for root, dirs, files in os.walk(SCAN_PATH):
            for file in files:
                if file.startswith('.'):
                    continue

                file_ext = os.path.splitext(file)[1].lower()
                if file_ext not in MEDIA_EXTENSIONS:
                    continue

                file_path = os.path.join(root, file)
                
                if SYMLINK_CHECK and is_broken_symlink(file_path):
                    handle_broken_symlink(file_path, stats)
                    continue

                stats.increment_scanned()

                if not is_in_plex(file_path):
                    library_id, library_title = get_library_id_for_path(file_path)
                    if library_title:
                        stats.add_missing_item(library_title, file_path)
                        logger.info(f"📁 Missing: {BOLD}{file_path}{RESET}")
                    
                        parent_folder = os.path.dirname(file_path)
                        if parent_folder not in scanned_folders and library_id:
                            scan_folder(library_id, parent_folder)
                            scanned_folders.add(parent_folder)

    asyncio.run(stats.send_discord_summary())

def main():
    logger.info("🚀 Plex Missing Files Scanner with Sonarr/Radarr Integration")
    logger.info(f"⏱️  Run interval: {BOLD}{RUN_INTERVAL}{RESET} hours")
    
    if RADARR_INSTANCES:
        logger.info(f"🎬 Radarr instances: {BOLD}{len(RADARR_INSTANCES)}{RESET}")
    if SONARR_INSTANCES:
        logger.info(f"📺 Sonarr instances: {BOLD}{len(SONARR_INSTANCES)}{RESET}")
    
    if RADARR_INSTANCES or SONARR_INSTANCES:
        status = "ENABLED" if ENABLE_BLOCKLIST else "DISABLED"
        color = GREEN if ENABLE_BLOCKLIST else YELLOW
        logger.info(f"{color}Blocklist: {BOLD}{status}{RESET}")
    
    logger.info("")
    
    run_scan()
    
    schedule.every(RUN_INTERVAL).hours.do(run_scan)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    if not os.path.exists('config.ini'):
        logger.error("❌ config.ini not found")
        exit(1)
    
    main()