import os, time, requests
from datetime import datetime, timezone

GH_API = "https://api.github.com"
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

GH_TOKEN = os.environ["NOTION_SEARCH_GH_TOKEN"]
NOTION_API_KEY = os.environ["NOTION_GH_ACTIVITY_API_KEY"]
NOTION_DB_ID = os.environ["NOTION_GH_ACTIVITY_DATABASE_ID"]
GH_USERNAME = os.environ["GH_USERNAME"]

headers_gh = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
}
headers_notion = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}


def gh_search_issues(query, per_page=100):
    """Yield items across all pages for GitHub issues/PR search."""
    page = 1
    while True:
        r = requests.get(
            f"{GH_API}/search/issues",
            headers=headers_gh,
            params={
                "q": query,
                "per_page": per_page,
                "page": page,
                "sort": "updated",
                "order": "desc",
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        if not items:
            break
        for it in items:
            yield it
        page += 1
        # basic rate-limit kindness
        time.sleep(0.3)


def gh_get_starred(username, per_page=100):
    """Yield starred repositories across all pages."""
    page = 1
    while True:
        r = requests.get(
            f"{GH_API}/users/{username}/starred",
            headers=headers_gh,
            params={"per_page": per_page, "page": page, "sort": "updated"},
            timeout=60,
        )
        r.raise_for_status()
        repos = r.json()
        if not repos:
            break
        for repo in repos:
            yield repo
        page += 1
        # basic rate-limit kindness
        time.sleep(0.3)


def notion_query_by_github_id(github_id):
    r = requests.post(
        f"{NOTION_API}/databases/{NOTION_DB_ID}/query",
        headers=headers_notion,
        json={
            "filter": {"property": "GitHub ID", "number": {"equals": github_id}},
            "page_size": 1,
        },
        timeout=60,
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None


def upsert_notion_page(item):
    # GitHub fields
    github_id = item["id"]
    title = item["title"] or ""
    url = item["html_url"]
    is_pr = "pull_request" in item
    typ = "PR" if is_pr else "Issue"
    number = item["number"]
    repo_full = (
        item["repository_url"].split("repos/")[-1] if "repository_url" in item else ""
    )
    state = item["state"]  # open/closed
    created_at = item["created_at"]
    updated_at = item["updated_at"]
    closed_at = item.get("closed_at")
    user_login = (item.get("user") or {}).get("login") or ""
    labels = [lab["name"] for lab in item.get("labels", []) if isinstance(lab, dict)]
    assignees = (
        [a["login"] for a in item.get("assignees", [])] if item.get("assignees") else []
    )
    # Best-effort involvement classification
    involvement = []
    if user_login.lower() == GH_USERNAME.lower():
        involvement.append("author")
    if assignees and GH_USERNAME in [a.lower() for a in assignees]:
        involvement.append("assignee")
    # Search results include involves:USERNAME, which also covers reviewer, commenter, etc.
    if "involves:" in item.get("_query_hint", ""):
        involvement.append("involves")

    props = {
        "Title": {"title": [{"text": {"content": title[:2000]}}]},
        "URL": {"url": url},
        "GitHub ID": {"number": github_id},
        "Type": {"select": {"name": typ}},
        "Number": {"number": number},
        "Repo": {"rich_text": [{"text": {"content": repo_full}}]},
        "State": {"select": {"name": state}},
        "Created At": {"date": {"start": created_at}},
        "Updated At": {"date": {"start": updated_at}},
        "Author": {"rich_text": [{"text": {"content": user_login}}]},
    }
    if closed_at:
        props["Closed At"] = {"date": {"start": closed_at}}
    if labels:
        props["Labels"] = {"multi_select": [{"name": label} for label in labels[:100]]}
    if assignees:
        props["Assignees"] = {
            "rich_text": [{"text": {"content": ", ".join(assignees[:50])}}]
        }
    if involvement:
        props["Involvement"] = {
            "rich_text": [{"text": {"content": ", ".join(sorted(set(involvement)))}}]
        }

    existing = notion_query_by_github_id(github_id)
    if existing:
        page_id = existing["id"]
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=headers_notion,
            json={"properties": props},
            timeout=60,
        )
        r.raise_for_status()
    else:
        r = requests.post(
            f"{NOTION_API}/pages",
            headers=headers_notion,
            json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
            timeout=60,
        )
        r.raise_for_status()


def upsert_starred_repo(repo):
    # GitHub repo fields
    github_id = repo["id"]
    name = repo["name"]
    full_name = repo["full_name"]
    url = repo["html_url"]
    description = repo.get("description") or ""
    created_at = repo["created_at"]
    updated_at = repo["updated_at"]
    pushed_at = repo.get("pushed_at")
    stargazers_count = repo.get("stargazers_count", 0)
    language = repo.get("language") or ""
    owner_login = repo["owner"]["login"]
    topics = repo.get("topics", [])

    props = {
        "Title": {"title": [{"text": {"content": name[:2000]}}]},
        "URL": {"url": url},
        "GitHub ID": {"number": github_id},
        "Type": {"select": {"name": "Star"}},
        "Repo": {"rich_text": [{"text": {"content": full_name}}]},
        "Description": {"rich_text": [{"text": {"content": description[:2000]}}]},
        "Created At": {"date": {"start": created_at}},
        "Updated At": {"date": {"start": updated_at}},
        "Stars": {"number": stargazers_count},
        "Language": {"rich_text": [{"text": {"content": language}}]},
        "Author": {"rich_text": [{"text": {"content": owner_login}}]},
    }
    if pushed_at:
        props["Pushed At"] = {"date": {"start": pushed_at}}
    if topics:
        props["Topics"] = {"multi_select": [{"name": topic} for topic in topics[:100]]}

    existing = notion_query_by_github_id(github_id)
    if existing:
        page_id = existing["id"]
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=headers_notion,
            json={"properties": props},
            timeout=60,
        )
        r.raise_for_status()
    else:
        r = requests.post(
            f"{NOTION_API}/pages",
            headers=headers_notion,
            json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
            timeout=60,
        )
        r.raise_for_status()


def main():
    # Two passes: PRs then issues. Use involves: to capture author/assignee/reviewer/activity.
    # We can filter by updated since last run to reduce work; simple full sync here for clarity.
    queries = [
        f"involves:{GH_USERNAME} is:pr archived:false",
        f"involves:{GH_USERNAME} is:issue archived:false",
    ]
    for q in queries:
        for item in gh_search_issues(q):
            # annotate for involvement hint
            item["_query_hint"] = f"search:{q}"
            upsert_notion_page(item)

    # Sync starred repositories
    for repo in gh_get_starred(GH_USERNAME):
        upsert_starred_repo(repo)


if __name__ == "__main__":
    main()
