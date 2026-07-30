import requests
import json
from urllib.parse import quote
import base64
import asyncio
import aiohttp
import re

# === Configuration ===
GITLAB_URL = "https://gitlab"   # Replace with your GitLab instance URL
api_token = ""

with open(".env", "r") as f:
        contents = f.read().splitlines()
        api_token = contents[1].split("=")[1]

headers = {"PRIVATE-TOKEN": api_token, "Accept": "application/json"}

# Rate limiting
CONCURRENT_LIMITS = 40
FILENAMES_TO_CHECK = ["package.json", "package-lock.json", "README.md", "pom.xml", "build.gradle", "build.gradle.kts"]
DEPENDENCIES_TO_FIND = ["fastjson", "joyfill"]
DEPENDENCY_REGEX = "|".join(map(re.escape, DEPENDENCIES_TO_FIND))

async def fetch_projects_page(session, page, semaphore):
    """Phase 1: Fetch a single page of projects concurrently."""
    url = f"{GITLAB_URL}/api/v4/projects?per_page=100&page={page}"
    headers = {"PRIVATE-TOKEN": api_token}
    
    async with semaphore:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    total_pages = int(response.headers.get("X-Total-Pages", 1))
                    data = await response.json()
                    
                    return [
                        {
                            "id": p["id"],
                            "path_with_namespace": p["path_with_namespace"]
                        }
                        for p in data
                    ], total_pages
                return [], 1
        except Exception:
            return [], 1

async def get_all_projects(session, semaphore):
    """Orchestrates pulling all projects across all pages."""
    print("📋 Phase 1: Fetching all project configurations...")
    first_page, total_pages = await fetch_projects_page(session, 1, semaphore)
    all_projects = list(first_page)
    
    if total_pages > 1:
        tasks = [fetch_projects_page(session, p, semaphore) for p in range(2, total_pages + 1)]
        pages_results = await asyncio.gather(*tasks)
        for page_data, _ in pages_results:
            all_projects.extend(page_data)
            
    print(f"✔️ Found {len(all_projects)} total projects.")
    return all_projects

async def get_project_branches(session, project, semaphore):
    """Phase 2: Fetch all available branch names for a specific project."""
    url = f"{GITLAB_URL}/api/v4/projects/{project['id']}/repository/branches"
    headers = {"PRIVATE-TOKEN": api_token}
    params = {"per_page": 100}
    
    async with semaphore:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    project["branches"] = [b["name"] for b in data]
                else:
                    project["branches"] = []
        except Exception:
            project["branches"] = []

async def check_file_for_dependency(session, project, branch, filename, semaphore, results_list):
    """Phase 3: Fetch file content via raw API and scan for the dependency string."""
    encoded_file = quote(filename, safe='')
    # Use the /raw endpoint to download plain text directly instead of base64 JSON
    url = f"{GITLAB_URL}/api/v4/projects/{project['id']}/repository/files/{encoded_file}/raw"
    headers = {"PRIVATE-TOKEN": api_token}
    params = {"ref": branch}
    async with semaphore:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    content = await response.text()
                    # Core Logic: Check if the explicitly set dependency exists in the file text
                    match = re.search(DEPENDENCY_REGEX, content)
                    if match:
                        matched = match.group()
                        output = f"🔥 [FOUND] {project['path_with_namespace']} | Branch: {branch} | File: {filename} | Contains: {matched}"
                        print(output)
                        results_list.append({
                            "id": project["id"],
                            "path_with_namespace": project["path_with_namespace"],
                            "branch": branch,
                            "filename": filename,
                            "dependency": matched
                        })
        except Exception:
            pass # Gracefully skip missing files (404) or timeouts to maintain speed

async def main_pipeline():
    # Bounded semaphore manages connection pools to avoid OS file descriptor limits and 429 errors
    network_bouncer = asyncio.BoundedSemaphore(CONCURRENT_LIMITS)
    
    async with aiohttp.ClientSession() as session:
        # Step 1: Collect all project configurations
        projects = await get_all_projects(session, network_bouncer)
        
        # Step 2: Grab all branches for all discovered projects in parallel
        print(f"\n🌿 Phase 2: Fetching available branches for {len(projects)} projects...")
        branch_tasks = [get_project_branches(session, proj, network_bouncer) for proj in projects]
        await asyncio.gather(*branch_tasks)
        
        # Step 3: Check contents of targets for the specific dependency
        print(f"\n🔍 Phase 3: Inspecting file contents for '{DEPENDENCIES_TO_FIND}' (Cap: {CONCURRENT_LIMITS})...")
        found_records = []
        content_scan_tasks = []
        
        # Matrix creation: Projects x Branches x Filenames
        for proj in projects:
            for branch in proj.get("branches", []):
                for filename in FILENAMES_TO_CHECK:
                    task = check_file_for_dependency(session, proj, branch, filename, network_bouncer, found_records)
                    content_scan_tasks.append(task)
        
        print(f"🚀 Firing {len(content_scan_tasks)} concurrent content download requests...")
        await asyncio.gather(*content_scan_tasks)
        
        print("\n📊 --- AUDIT COMPLETE ---")
        print(f"Scanned matrix cells. Found '{DEPENDENCIES_TO_FIND}' in {len(found_records)} places.")

if __name__ == "__main__":
    asyncio.run(main_pipeline())
