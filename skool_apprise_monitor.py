#!/usr/bin/env python3
"""
Skool Apprise Monitor v2 — Enrichment-before-alert community intelligence.

Monitors Skool communities for:
  1. New ICP members → enrich (Perplexity + Tavily LinkedIn) → notify with links
  2. Cancelled members (churn) → alert if they were ICP qualified
  3. Financial wins (regex pattern matching on posts)
  4. Anti-gravity mentions (brand mentions in posts)
  5. Meaningful @florian tags (filters out "thanks" noise)

Every event is logged to daily JSONL for the nightly digest.

Usage:
    # Daemon mode — full monitoring every 3 minutes:
    python execution/skool_apprise_monitor.py --daemon --interval 180

    # Single run:
    python execution/skool_apprise_monitor.py

    # Dry run (no notifications sent):
    python execution/skool_apprise_monitor.py --dry-run

    # Initialize state (first run, no notifications):
    python execution/skool_apprise_monitor.py --init

    # Show browser for debugging:
    python execution/skool_apprise_monitor.py --visible

Requires:
    pip install playwright requests python-dotenv
    playwright install chromium

Environment (.env):
    SKOOL_AUTH_TOKEN       — Skool session cookie (required)
    APPRISE_URL            — Apprise API base URL (e.g. https://notify.florianrolke.com)
    APPRISE_URLS           — Notification URLs (e.g. ntfy://ntfy.sh/skool-icp-cb311748)
    PERPLEXITY_API_KEY     — For member enrichment
    TAVILY_API_KEY_5       — For LinkedIn finder (keys 1-4 exhausted)
    TAVILY_API_KEY_6       — Fallback LinkedIn finder key
"""

import sys
import os
import re
import json
import asyncio
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Docker may not have dotenv, env vars set directly

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent.parent
# In Docker container, use /app/state; locally use .tmp/apprise_state
STATE_DIR = Path(os.getenv("STATE_DIR", str(BASE_DIR / ".tmp" / "apprise_state")))
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Event log directory
EVENTS_DIR = Path(os.getenv("EVENTS_DIR", str(STATE_DIR / "events")))
EVENTS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CONFIGURATION
# ============================================================================

COMMUNITY = os.getenv("COMMUNITY", "aiautomationsbyjack")
APPRISE_URL = os.getenv("APPRISE_URL", "https://notify.florianrolke.com")

# API keys for enrichment
PERPLEXITY_KEY = os.getenv("PERPLEXITY_API_KEY", "")

# Tavily keys — start with key 5 (1-4 exhausted as of Feb 2026)
TAVILY_KEYS = []
for suffix in ["_5", "_6", "", "_2", "_3", "_4"]:
    key = os.getenv(f"TAVILY_API_KEY{suffix}", "")
    if key:
        TAVILY_KEYS.append(key)
_tavily_idx = 0
_tavily_exhausted = set()

# Mention keywords
MENTION_KEYWORDS = ["@florian", "florian rolke"]

# Anti-gravity / brand keywords
ANTIGRAVITY_KEYWORDS = [
    "anti-gravity", "antigravity", "anti gravity",
    "florianrolke", "florian rolke",
]

# Money patterns
MONEY_PATTERNS = [
    r'\$[\d,]+k?',
    r'(?:closed|signed|landed).{0,20}\$',
    r'(?:revenue|deal|contract).{0,30}\$',
    r'(?:6|7|six|seven)-?figure',
    r'(?:first|biggest).{0,20}(?:client|deal|sale)',
    r'(?:won|landed|secured).{0,20}(?:client|deal|contract)',
]

# Gratitude words for tag filtering
GRATITUDE_WORDS = {
    "thanks", "thank you", "thx", "ty", "appreciate", "appreciated",
    "grateful", "kudos", "cheers", "shoutout", "shout out",
}

# Discussion indicators for meaningful tag detection
DISCUSSION_INDICATORS = {
    "how", "what", "why", "when", "where", "which", "who",
    "anyone", "question", "help", "advice", "thoughts",
    "opinion", "recommend", "suggestion", "idea", "strategy",
    "struggling", "issue", "problem", "challenge",
}


def build_apprise_urls():
    """Build Apprise notification URLs from env vars."""
    urls = []
    email_to = os.getenv("APPRISE_EMAIL_TO", os.getenv("NOTIFY_EMAIL_TO", ""))
    smtp_user = os.getenv("SMTP_USER", os.getenv("NOTIFY_EMAIL_FROM", ""))
    smtp_pass = os.getenv("SMTP_PASS", os.getenv("NOTIFY_EMAIL_PASSWORD", ""))
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    if email_to and smtp_user and smtp_pass:
        urls.append(f"mailto://{smtp_user}:{smtp_pass}@{smtp_host}?to={email_to}")
    custom = os.getenv("APPRISE_URLS", "")
    if custom:
        urls.extend([u.strip() for u in custom.split(",") if u.strip()])
    return urls


# ============================================================================
# PERSISTENT BROWSER SESSION (for daemon mode)
# ============================================================================

class BrowserSession:
    """Manages a persistent Playwright browser for repeated scraping cycles."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self._cycle_count = 0
        self._restart_every = 50

    async def start(self):
        from playwright.async_api import async_playwright
        auth_token = os.getenv("SKOOL_AUTH_TOKEN")
        client_id = os.getenv("SKOOL_CLIENT_ID", "")
        if not auth_token:
            raise ValueError("SKOOL_AUTH_TOKEN not set in .env")
        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        cookies = [
            {'name': 'auth_token', 'value': auth_token, 'domain': '.skool.com', 'path': '/'},
        ]
        if client_id:
            cookies.append({'name': 'client_id', 'value': client_id, 'domain': '.skool.com', 'path': '/'})
        await self.context.add_cookies(cookies)
        self.page = await self.context.new_page()
        self.page.set_default_timeout(60000)
        self._cycle_count = 0
        print(f"  Browser session started (headless={self.headless})")

    async def stop(self):
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
        self.page = self.context = self.browser = self._playwright = None

    async def maybe_restart(self):
        self._cycle_count += 1
        if self._cycle_count >= self._restart_every:
            print(f"  Restarting browser (cycle {self._cycle_count})...")
            await self.stop()
            await self.start()

    @property
    def is_alive(self) -> bool:
        return self.page is not None and self.browser is not None


# ============================================================================
# INLINE ENRICHMENT — Perplexity + Tavily LinkedIn
# ============================================================================

def _get_tavily_key():
    """Get next available Tavily key, skipping exhausted ones."""
    global _tavily_idx
    if not TAVILY_KEYS:
        return None
    for _ in range(len(TAVILY_KEYS)):
        idx = _tavily_idx % len(TAVILY_KEYS)
        _tavily_idx += 1
        if idx not in _tavily_exhausted:
            return TAVILY_KEYS[idx]
    return None


LINKEDIN_PATTERN = re.compile(
    r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/([a-zA-Z0-9\-_%]+)',
    re.IGNORECASE
)


def _extract_linkedin_url(text: str):
    """Extract first valid LinkedIn /in/ URL from text."""
    if not text:
        return None
    match = LINKEDIN_PATTERN.search(text)
    if match:
        slug = match.group(1).rstrip('/')
        if slug.lower() in ('example', 'username', 'yourname', 'profile', '', 'dir'):
            return None
        return f"https://www.linkedin.com/in/{slug}"
    return None


def _validate_linkedin_for_person(url: str, name: str) -> bool:
    """Check that a LinkedIn URL likely belongs to this person."""
    if not url or not name:
        return False
    slug = url.split('/in/')[-1].lower().rstrip('/')
    slug_clean = re.sub(r'-[0-9a-f]{6,}$', '', slug)
    name_parts = [p.lower() for p in name.split() if len(p) > 1]
    if not name_parts:
        return False
    if len(name_parts) >= 2:
        last = name_parts[-1]
        if last not in slug_clean:
            return False
        first = name_parts[0]
        return first in slug_clean or first[:3] in slug_clean
    return name_parts[0] in slug_clean


def enrich_with_perplexity(name: str, bio: str, profile_url: str) -> dict:
    """Quick Perplexity enrichment — extract company, website, services, city."""
    if not PERPLEXITY_KEY:
        return {}

    bio_context = f'\nTheir Skool bio says: "{bio}"' if bio and len(bio.strip()) > 3 else ""
    prompt = f"""Research this person and extract business intelligence. They are a member of an AI automation community on Skool.

Name: {name}
Skool profile: {profile_url}{bio_context}

Extract and return as JSON only (no markdown, no explanation):
{{
    "company_name": "their company name or null",
    "company_description": "1-2 sentence description or null",
    "website": "personal/business website if found or null",
    "city": "city name or null",
    "country": "country name or null",
    "services": ["list of services they offer"],
    "industries": ["industries/niches they serve"],
    "linkedin_url": "LinkedIn profile URL if found or null",
    "confidence": "high/medium/low"
}}

Only include factual, verifiable information. If you can't find much, mark confidence as low."""

    try:
        response = requests.post(
            'https://api.perplexity.ai/chat/completions',
            headers={
                'Authorization': f'Bearer {PERPLEXITY_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'sonar',
                'messages': [
                    {'role': 'system', 'content': 'You are a business research assistant. Return valid JSON only, no markdown code blocks.'},
                    {'role': 'user', 'content': prompt}
                ],
                'temperature': 0.1,
                'max_tokens': 600
            },
            timeout=15
        )
        if response.status_code == 200:
            content = response.json().get('choices', [{}])[0].get('message', {}).get('content', '')
            # Parse JSON from response
            content = content.strip()
            if content.startswith('```'):
                content = re.sub(r'^```(?:json)?\s*', '', content)
                content = re.sub(r'\s*```$', '', content)
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                # Try to find JSON object in text
                match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(0))
                    except json.JSONDecodeError:
                        pass
            return {}
        elif response.status_code == 429:
            print(f"    Perplexity rate limited")
            return {}
        else:
            print(f"    Perplexity error: {response.status_code}")
            return {}
    except Exception as e:
        print(f"    Perplexity exception: {e}")
        return {}


def find_linkedin_tavily(name: str, company: str = "", location: str = "") -> str | None:
    """Search Tavily for LinkedIn profile URL. Returns normalized URL or None."""
    key = _get_tavily_key()
    if not key:
        return None

    query_parts = [f'"{name}"']
    if company:
        query_parts.append(f'"{company}"')
    if location:
        city = location.split('(')[0].strip() if '(' in location else location
        if city:
            query_parts.append(city)

    try:
        response = requests.post(
            'https://api.tavily.com/search',
            json={
                'api_key': key,
                'query': " ".join(query_parts),
                'search_depth': 'basic',
                'max_results': 5,
                'include_answer': False,
                'include_domains': ['linkedin.com']
            },
            timeout=15
        )

        if response.status_code == 200:
            results = response.json().get('results', [])
            for r in results:
                li_url = _extract_linkedin_url(r.get('url', ''))
                if li_url and _validate_linkedin_for_person(li_url, name):
                    return li_url
            # Check content for embedded URLs
            for r in results:
                li_url = _extract_linkedin_url(r.get('content', ''))
                if li_url and _validate_linkedin_for_person(li_url, name):
                    return li_url
            return None
        elif response.status_code in (429, 432):
            idx = (_tavily_idx - 1) % len(TAVILY_KEYS)
            _tavily_exhausted.add(idx)
            print(f"    Tavily key {idx+1} exhausted, rotating...")
            return find_linkedin_tavily(name, company, location)
        else:
            print(f"    Tavily error: {response.status_code}")
            return None
    except Exception as e:
        print(f"    Tavily exception: {e}")
        return None


def enrich_member(member: dict) -> dict:
    """
    Full inline enrichment: Perplexity → Tavily LinkedIn.

    Returns enrichment dict with: company, website, linkedin, city, etc.
    This runs BEFORE notification so links are ready.
    """
    name = member.get("name", "")
    bio = member.get("bio", "")
    profile_url = member.get("profileUrl", "")

    enrichment = {"enriched_at": datetime.now(timezone.utc).isoformat()}

    # Step 1: Perplexity quick enrichment
    print(f"    Enriching {name}...")
    perplexity_data = enrich_with_perplexity(name, bio, profile_url)
    if perplexity_data:
        enrichment["company"] = perplexity_data.get("company_name") or ""
        enrichment["company_description"] = perplexity_data.get("company_description") or ""
        enrichment["website"] = perplexity_data.get("website") or ""
        enrichment["city"] = perplexity_data.get("city") or ""
        enrichment["country"] = perplexity_data.get("country") or ""
        enrichment["services"] = perplexity_data.get("services") or []
        enrichment["industries"] = perplexity_data.get("industries") or []
        enrichment["confidence"] = perplexity_data.get("confidence", "low")
        # Perplexity might find LinkedIn incidentally
        perplexity_linkedin = perplexity_data.get("linkedin_url")
        if perplexity_linkedin:
            li_url = _extract_linkedin_url(perplexity_linkedin)
            if li_url:
                enrichment["linkedin"] = li_url
                enrichment["linkedin_source"] = "perplexity"

    # Step 2: Dedicated Tavily LinkedIn finder (if Perplexity didn't find it)
    if not enrichment.get("linkedin"):
        company = enrichment.get("company", "")
        location = enrichment.get("city", "")
        linkedin_url = find_linkedin_tavily(name, company, location)
        if linkedin_url:
            enrichment["linkedin"] = linkedin_url
            enrichment["linkedin_source"] = "tavily"

    found = []
    if enrichment.get("linkedin"):
        found.append("LinkedIn")
    if enrichment.get("website"):
        found.append("website")
    if enrichment.get("company"):
        found.append("company")
    print(f"    Enriched: {', '.join(found) if found else 'minimal data'}")

    return enrichment


# ============================================================================
# ICP SCORING
# ============================================================================

TIER_A_SIGNALS = {
    "position": [
        "agency owner", "founder", "ceo", "owner", "co-founder",
        "managing director", "president", "principal",
    ],
    "industry": [
        "construction", "plumbing", "hvac", "electrical", "roofing",
        "manufacturing", "contractor", "real estate", "insurance",
        "landscaping", "dental", "medical", "legal", "accounting",
    ],
    "revenue": [
        "7 figure", "7-figure", "multiple 6 figure", "million",
        "$1m", "$2m", "$5m", "$10m", "revenue",
    ],
    "pain": [
        "scaling", "leads", "struggling with", "need help",
        "looking for", "want to grow", "automate", "systems",
    ],
}

TIER_B_SIGNALS = {
    "position": [
        "consultant", "freelancer", "coach", "entrepreneur",
        "business owner", "self-employed", "cmo", "vp",
    ],
    "industry": [
        "marketing", "saas", "e-commerce", "ecommerce", "fitness",
        "health", "wellness", "education", "tech", "software",
    ],
}

AI_AGENCY_KEYWORDS = [
    "ai agency", "ai automation", "chatgpt", "claude", "llm",
    "machine learning", "prompt engineer", "ai consultant",
    "ai solutions", "ai services", "ai integration",
]


def quick_score_member(member: dict, enrichment: dict = None) -> dict:
    """Keyword-based ICP scoring. Uses bio + enrichment data for scoring."""
    name = (member.get("name", "") or "").lower()
    bio = (member.get("bio", "") or "").lower()

    # Include enrichment data in scoring text
    extra_text = ""
    if enrichment:
        extra_text = " ".join([
            enrichment.get("company", ""),
            enrichment.get("company_description", ""),
            " ".join(enrichment.get("services", [])),
            " ".join(enrichment.get("industries", [])),
        ]).lower()

    text = f"{name} {bio} {extra_text}"

    score = 0
    reasons = []

    for kw in AI_AGENCY_KEYWORDS:
        if kw in text:
            member["icp_score"] = 0
            member["tier"] = "D"
            member["match_reasons"] = ["AI agency — not ICP"]
            return member

    for category, keywords in TIER_A_SIGNALS.items():
        for kw in keywords:
            if kw in text:
                weight = {"position": 25, "industry": 15, "revenue": 15, "pain": 10}[category]
                score += weight
                reasons.append(f"{category.title()}: {kw}")
                break

    for category, keywords in TIER_B_SIGNALS.items():
        for kw in keywords:
            if kw in text:
                weight = {"position": 15, "industry": 10}[category]
                score += weight
                reasons.append(f"{category.title()}: {kw}")
                break

    if bio and len(bio) > 20:
        score += 5
        reasons.append("Has detailed bio")

    score = min(score, 100)
    tier = "A" if score >= 60 else "B" if score >= 35 else "C" if score >= 15 else "D"

    member["icp_score"] = score
    member["tier"] = tier
    member["match_reasons"] = reasons
    return member


# ============================================================================
# DETECTION: WINS + MENTIONS + MEANINGFUL TAGS
# ============================================================================

def detect_money_pattern(text: str):
    """Detect monetary win patterns in text."""
    text_lower = text.lower()
    for pattern in MONEY_PATTERNS:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            return match.group(0)
    return None


def is_meaningful_mention(post: dict) -> bool:
    """
    Filter out noise mentions. A mention is meaningful if:
    - Post is > 15 words (excluding the tag itself)
    - Contains a question mark OR discussion indicator words
    - Is NOT purely gratitude
    """
    content = (post.get('content', '') or post.get('title', '')).strip()
    if not content:
        return False

    # Remove the tag itself for word counting
    cleaned = re.sub(r'@\w+', '', content).strip()
    words = cleaned.split()
    word_count = len(words)

    # Must be substantial enough
    if word_count < 15:
        return False

    content_lower = cleaned.lower()

    # Check if it's purely gratitude
    gratitude_count = sum(1 for w in GRATITUDE_WORDS if w in content_lower)
    non_gratitude_words = [w for w in words if w.lower().strip('.,!?') not in GRATITUDE_WORDS]
    if gratitude_count > 0 and len(non_gratitude_words) < 10:
        return False

    # Must contain a question or discussion indicator
    has_question = '?' in content
    has_discussion = any(indicator in content_lower for indicator in DISCUSSION_INDICATORS)

    return has_question or has_discussion


def detect_mentions(posts: list) -> list:
    """Detect @mentions and name mentions of Florian in posts. Filters for meaningful ones."""
    mentions = []
    for post in posts:
        content = post.get('content', '') or post.get('title', '')
        content_lower = content.lower()

        for keyword in MENTION_KEYWORDS:
            if keyword.lower() in content_lower:
                meaningful = is_meaningful_mention(post)
                pos = content_lower.find(keyword.lower())
                start = max(0, pos - 80)
                end = min(len(content), pos + len(keyword) + 80)
                context = content[start:end].strip()
                if start > 0:
                    context = '...' + context
                if end < len(content):
                    context = context + '...'

                mentions.append({
                    'post_id': post.get('id') or post.get('postId'),
                    'type': '@mention' if keyword.startswith('@') else 'name_mention',
                    'keyword': keyword,
                    'meaningful': meaningful,
                    'author_name': post.get('authorName') or post.get('author', {}).get('name', 'Unknown'),
                    'author_handle': post.get('author', {}).get('username', '') or post.get('author', {}).get('name', ''),
                    'post_title': post.get('title', ''),
                    'post_url': post.get('url') or post.get('postUrl', ''),
                    'context': context,
                    'likes_count': post.get('likesCount', 0),
                    'comments_count': post.get('commentsCount', 0),
                })
                break
    return mentions


def detect_antigravity_mentions(posts: list) -> list:
    """Detect anti-gravity/brand mentions in posts (separate from @mentions)."""
    mentions = []
    for post in posts:
        content = (post.get('content', '') or '') + ' ' + (post.get('title', '') or '')
        content_lower = content.lower()

        for keyword in ANTIGRAVITY_KEYWORDS:
            if keyword in content_lower:
                pos = content_lower.find(keyword)
                start = max(0, pos - 60)
                end = min(len(content), pos + len(keyword) + 60)
                context = content[start:end].strip()

                mentions.append({
                    'post_id': post.get('id') or post.get('postId'),
                    'keyword': keyword,
                    'author_name': post.get('authorName') or post.get('author', {}).get('name', 'Unknown'),
                    'author_handle': post.get('author', {}).get('username', ''),
                    'post_title': post.get('title', ''),
                    'post_url': post.get('url') or post.get('postUrl', ''),
                    'context': context,
                })
                break
    return mentions


def detect_wins(posts: list) -> list:
    """Detect financial wins in posts."""
    wins = []
    for post in posts:
        content = post.get('content', '') or post.get('title', '')
        money_match = detect_money_pattern(content)
        if money_match:
            wins.append({
                'post_id': post.get('id') or post.get('postId'),
                'title': post.get('title', ''),
                'author_name': post.get('authorName') or post.get('author', {}).get('name', 'Unknown'),
                'author_handle': post.get('author', {}).get('username', ''),
                'money_pattern': money_match,
                'post_url': post.get('url') or post.get('postUrl', ''),
                'likes_count': post.get('likesCount', 0),
                'comments_count': post.get('commentsCount', 0),
            })
    return wins


# ============================================================================
# MEMBER SCRAPING
# ============================================================================

def _parse_members_from_next_data(next_data: dict, community: str, seen_handles: set) -> list:
    """Extract member dicts from __NEXT_DATA__ JSON."""
    members = []
    page_props = next_data.get('props', {}).get('pageProps', {}) or {}
    member_list = (
        page_props.get('members', []) or
        page_props.get('groupMembers', []) or
        page_props.get('users', []) or []
    )

    if not member_list:
        dehydrated = page_props.get('dehydratedState', {})
        if dehydrated:
            for q in dehydrated.get('queries', []):
                data = q.get('state', {}).get('data', {})
                if isinstance(data, dict):
                    items = data.get('items', []) or data.get('members', []) or data.get('data', [])
                    if items and isinstance(items, list) and len(items) > 0:
                        first = items[0]
                        if isinstance(first, dict) and ('name' in first or 'user' in first):
                            member_list = items
                            break
                elif isinstance(data, list) and len(data) > 0:
                    first = data[0]
                    if isinstance(first, dict) and ('name' in first or 'user' in first):
                        member_list = data
                        break

    for m in member_list:
        if m.get('firstName') or m.get('lastName'):
            first = m.get("firstName", "")
            last = m.get("lastName", "")
            full_name = f"{first} {last}".strip() or m.get("name", "")
            handle = m.get("name", "")
            meta = m.get("metadata", {}) or {}
            bio = meta.get("bio", "") or ""
            member_data = {
                "name": full_name,
                "handle": handle,
                "bio": bio,
                "profileUrl": f"https://www.skool.com/@{handle}",
                "joinedAt": m.get("createdAt", "") or m.get("joinedAt", ""),
            }
        elif 'user' in m and isinstance(m['user'], dict):
            user = m['user']
            member_data = {
                "name": user.get("name", ""),
                "handle": user.get("username", "") or user.get("handle", ""),
                "bio": user.get("bio", "") or m.get("bio", ""),
                "profileUrl": f"https://www.skool.com/@{user.get('username', '')}",
                "joinedAt": m.get("createdAt", "") or m.get("joinedAt", ""),
            }
        else:
            member_data = {
                "name": m.get("name", ""),
                "handle": m.get("username", "") or m.get("handle", "") or m.get("name", ""),
                "bio": m.get("bio", ""),
                "profileUrl": f"https://www.skool.com/@{m.get('username', m.get('handle', m.get('name', '')))}",
                "joinedAt": m.get("createdAt", "") or m.get("joinedAt", ""),
            }

        handle = member_data["handle"].lower()
        if handle and handle not in seen_handles:
            seen_handles.add(handle)
            member_data["community"] = community
            members.append(member_data)

    return members


async def scrape_members_with_page(page, community: str, max_pages: int = 1) -> list:
    """Scrape newest members using an existing Playwright page."""
    members = []
    seen_handles = set()

    for page_num in range(1, max_pages + 1):
        url = f"https://www.skool.com/{community}/-/members?sort=newest"
        if page_num > 1:
            url += f"&p={page_num}"

        print(f"  [{page_num}/{max_pages}] Loading members page...")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector('#__NEXT_DATA__', state='attached', timeout=15000)
        await page.wait_for_timeout(2000)

        next_data = await page.evaluate("""
            () => {
                const el = document.getElementById('__NEXT_DATA__');
                return el ? JSON.parse(el.textContent) : null;
            }
        """)

        if next_data:
            page_members = _parse_members_from_next_data(next_data, community, seen_handles)
            members.extend(page_members)
            print(f"    Found {len(page_members)} on page ({len(members)} total unique)")

        if page_num < max_pages:
            await asyncio.sleep(5)

    return members


async def scrape_member_list(community: str, max_pages: int = 2,
                              headless: bool = True) -> list:
    """Scrape newest members (standalone browser)."""
    session = BrowserSession(headless=headless)
    await session.start()
    try:
        return await scrape_members_with_page(session.page, community, max_pages)
    finally:
        await session.stop()


# ============================================================================
# CANCELLING TAB SCRAPER — detects paid cancellations
# ============================================================================

def _parse_cancelling_from_next_data(next_data: dict, community: str) -> list:
    """
    Parse members from the Cancelling tab's __NEXT_DATA__.

    Each member has subscription status info. We extract:
    - Basic member data (name, handle, bio)
    - Whether it's a trial cancellation or paid cancellation
    - Days until churn
    - Price/plan info

    From screenshots, Skool shows:
    - "Trial cancelled (removing in 1 day)" → trial, skip
    - "Cancelled (churns in 25 days)" → paid, ALERT
    """
    members = []
    page_props = next_data.get('props', {}).get('pageProps', {}) or {}

    # Try all possible member list locations
    member_list = (
        page_props.get('members', []) or
        page_props.get('groupMembers', []) or
        page_props.get('users', []) or []
    )

    # Check dehydratedState as fallback
    if not member_list:
        dehydrated = page_props.get('dehydratedState', {})
        if dehydrated:
            for q in dehydrated.get('queries', []):
                data = q.get('state', {}).get('data', {})
                if isinstance(data, dict):
                    items = data.get('items', []) or data.get('members', []) or data.get('data', [])
                    if items and isinstance(items, list) and len(items) > 0:
                        first = items[0]
                        if isinstance(first, dict) and ('name' in first or 'user' in first or 'firstName' in first):
                            member_list = items
                            break
                elif isinstance(data, list) and len(data) > 0:
                    first = data[0]
                    if isinstance(first, dict) and ('name' in first or 'user' in first or 'firstName' in first):
                        member_list = data
                        break

    for m in member_list:
        # Extract basic member info (same parsing as active members)
        if m.get('firstName') or m.get('lastName'):
            first = m.get("firstName", "")
            last = m.get("lastName", "")
            full_name = f"{first} {last}".strip() or m.get("name", "")
            handle = m.get("name", "")
            meta = m.get("metadata", {}) or {}
            bio = meta.get("bio", "") or ""
        elif 'user' in m and isinstance(m['user'], dict):
            user = m['user']
            full_name = user.get("name", "")
            handle = user.get("username", "") or user.get("handle", "")
            bio = user.get("bio", "") or m.get("bio", "")
        else:
            full_name = m.get("name", "")
            handle = m.get("username", "") or m.get("handle", "") or m.get("name", "")
            bio = m.get("bio", "")

        if not handle:
            continue

        # Extract subscription/cancellation status from all possible fields
        member_meta = m.get("metadata", {}) or {}
        member_obj = m.get("member", {}) or {}
        member_member_meta = member_obj.get("metadata", {}) or {}

        # Look for cancellation indicators in various Skool data shapes
        # The key is distinguishing "Trial cancelled" from "Cancelled" (paid)
        is_trial = False
        cancel_status = ""
        price = ""
        joined_at = m.get("createdAt", "") or m.get("joinedAt", "") or member_obj.get("createdAt", "")

        # Check for trial indicators in all available fields
        # Skool may use: trialEnd, isTrial, subscriptionStatus, cancelReason, etc.
        all_fields = {**m, **member_meta, **member_obj, **member_member_meta}

        # Check for explicit trial flags
        if all_fields.get("isTrial") or all_fields.get("is_trial"):
            is_trial = True
        if all_fields.get("trialEnd") or all_fields.get("trial_end"):
            is_trial = True
        # subscription object may contain plan details
        sub = all_fields.get("subscription", {}) or {}
        if isinstance(sub, dict):
            if sub.get("trial") or sub.get("isTrial"):
                is_trial = True
            price = sub.get("price", "") or sub.get("amount", "")
            cancel_status = sub.get("status", "") or sub.get("cancelStatus", "")

        # Check cancelledAt vs trialCancelledAt
        if all_fields.get("trialCancelledAt") or all_fields.get("trialCanceledAt"):
            is_trial = True
        if all_fields.get("cancelledAt") or all_fields.get("canceledAt"):
            if not is_trial:
                cancel_status = "cancelled"

        # Check the status/memberStatus field
        status = (all_fields.get("status", "") or all_fields.get("memberStatus", "") or "").lower()
        if "trial" in status:
            is_trial = True

        members.append({
            "name": full_name,
            "handle": handle.lower(),
            "bio": bio,
            "profileUrl": f"https://www.skool.com/@{handle}",
            "community": community,
            "joinedAt": joined_at,
            "is_trial": is_trial,
            "cancel_status": cancel_status,
            "price": str(price),
            "raw_fields": {
                k: str(v)[:200] for k, v in all_fields.items()
                if k not in ('metadata', 'member', 'user', 'dehydratedState')
                and v is not None and str(v).strip()
            },
        })

    return members


async def scrape_cancelling_with_page(page, community: str, max_pages: int = 2) -> list:
    """
    Scrape the Cancelling tab to detect members who cancelled their subscription.

    URL: /{community}/-/members?tab=cancelling&sort=newest
    """
    all_cancelling = []
    seen_handles = set()

    for page_num in range(1, max_pages + 1):
        url = f"https://www.skool.com/{community}/-/members?tab=cancelling&sort=newest"
        if page_num > 1:
            url += f"&p={page_num}"

        print(f"  [Cancelling {page_num}/{max_pages}] Loading...")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector('#__NEXT_DATA__', state='attached', timeout=15000)
        await page.wait_for_timeout(2000)

        next_data = await page.evaluate("""
            () => {
                const el = document.getElementById('__NEXT_DATA__');
                return el ? JSON.parse(el.textContent) : null;
            }
        """)

        if next_data:
            # On first page, dump a sample of raw data to help debug field names
            if page_num == 1:
                page_props = next_data.get('props', {}).get('pageProps', {}) or {}
                sample_list = (
                    page_props.get('members', []) or
                    page_props.get('groupMembers', []) or
                    page_props.get('users', []) or []
                )
                if not sample_list:
                    dehydrated = page_props.get('dehydratedState', {})
                    if dehydrated:
                        for q in dehydrated.get('queries', []):
                            data = q.get('state', {}).get('data', {})
                            if isinstance(data, dict):
                                items = data.get('items', []) or data.get('members', []) or []
                                if items:
                                    sample_list = items
                                    break
                if sample_list:
                    first = sample_list[0]
                    # Log field keys from first cancelling member for debugging
                    print(f"    Cancelling member fields: {list(first.keys())[:15]}")
                    member_sub = first.get("member", {})
                    if member_sub:
                        print(f"    member sub-fields: {list(member_sub.keys())[:15]}")
                    meta = first.get("metadata", {})
                    if meta:
                        print(f"    metadata fields: {list(meta.keys())[:10]}")

            members = _parse_cancelling_from_next_data(next_data, community)
            for m in members:
                h = m["handle"]
                if h not in seen_handles:
                    seen_handles.add(h)
                    all_cancelling.append(m)

            print(f"    Found {len(members)} cancelling members (page {page_num})")

        if page_num < max_pages:
            await asyncio.sleep(5)

    return all_cancelling


def detect_new_cancellations(cancelling_members: list, community: str,
                              enrichment_cache: dict) -> list:
    """
    Detect NEW paid cancellations from the Cancelling tab scrape.

    Compares against previously seen cancelling handles to only alert once per member.
    Filters out trial cancellations — only alerts for paid $77/month cancellations.
    On first run (no state file yet), seeds state silently — no flood of old cancellations.

    Returns list of cancellation dicts ready for ICP scoring + notification.
    """
    state_path = STATE_DIR / f"cancelling_{community}.json"

    # First run: seed all current handles without alerting (prevents flood on deploy)
    if not state_path.exists():
        cancel_state = {"seen_ids": []}
        all_handles = [m["handle"] for m in cancelling_members if m.get("handle")]
        add_to_state(cancel_state, all_handles)
        save_state("cancelling", community, cancel_state)
        print(f"  Cancelling state initialized: {len(all_handles)} handles (no alerts on first run)")
        return []

    cancel_state = load_state("cancelling", community)
    seen_cancelling = set(cancel_state.get("seen_ids", []))

    new_cancellations = []

    for member in cancelling_members:
        handle = member["handle"]

        # Skip if we already alerted for this cancellation
        if handle in seen_cancelling:
            continue

        # Skip trial cancellations — we only care about PAID cancellations
        if member.get("is_trial"):
            print(f"    Skip trial cancel: {member['name']} ({handle})")
            seen_cancelling.add(handle)
            continue

        # This is a new paid cancellation — prepare for ICP scoring + notification
        cached = enrichment_cache.get(handle, {})
        enrichment = cached.get("enrichment", {})

        new_cancellations.append({
            "handle": handle,
            "name": member.get("name", handle),
            "bio": member.get("bio", ""),
            "profileUrl": member.get("profileUrl", f"https://www.skool.com/@{handle}"),
            "joinedAt": member.get("joinedAt", ""),
            "tier": cached.get("tier", "unknown"),
            "icp_score": cached.get("icp_score", 0),
            "enrichment": enrichment,
            "raw_fields": member.get("raw_fields", {}),
        })

    # Update state with ALL cancelling handles (trial + paid) to avoid re-processing
    all_handles = [m["handle"] for m in cancelling_members]
    add_to_state(cancel_state, all_handles)
    save_state("cancelling", community, cancel_state)

    return new_cancellations


# ============================================================================
# POST SCRAPING
# ============================================================================

async def scrape_posts_with_page(page, community: str, max_pages: int = 2) -> list:
    """Scrape recent posts using an existing Playwright page."""
    posts = []
    seen_ids = set()

    for page_num in range(1, max_pages + 1):
        url = f"https://www.skool.com/{community}?p={page_num}"

        print(f"  [{page_num}/{max_pages}] Loading posts page...")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector('#__NEXT_DATA__', state='attached', timeout=15000)
        await page.wait_for_timeout(2000)

        next_data = await page.evaluate("""
            () => {
                const el = document.getElementById('__NEXT_DATA__');
                return el ? JSON.parse(el.textContent) : null;
            }
        """)

        if next_data:
            page_props = next_data.get('props', {}).get('pageProps', {}) or {}

            # Skool uses postTrees[].post with metadata for content
            post_trees = page_props.get('postTrees', []) or []
            post_list = page_props.get('posts', []) or []

            if post_trees:
                # Current Skool format: postTrees[].post.metadata.{title,content,upvotes,comments}
                for tree in post_trees:
                    raw = tree.get('post', {}) or {}
                    post_id = raw.get('id', '')
                    if not post_id or post_id in seen_ids:
                        continue
                    seen_ids.add(post_id)

                    meta = raw.get('metadata', {}) or {}
                    user = raw.get('user', {}) or {}
                    slug = raw.get('name', '')
                    first_name = user.get('firstName', '')
                    last_name = user.get('lastName', '')
                    author_name = f"{first_name} {last_name}".strip() or user.get('name', '')

                    posts.append({
                        'id': post_id,
                        'title': meta.get('title', '') or '',
                        'content': meta.get('content', '') or '',
                        'authorName': author_name,
                        'author': {'name': user.get('name', ''), 'username': user.get('name', '')},
                        'url': f"https://www.skool.com/{community}/{slug}" if slug else f"https://www.skool.com/{community}/{post_id}",
                        'postUrl': f"https://www.skool.com/{community}/{slug}" if slug else f"https://www.skool.com/{community}/{post_id}",
                        'likesCount': meta.get('upvotes', 0) or 0,
                        'commentsCount': meta.get('comments', 0) or 0,
                        'createdAt': raw.get('createdAt', ''),
                        'categoryName': '',  # labels ID needs separate lookup
                    })

                print(f"    Found {len(post_trees)} posts ({len(posts)} total unique)")

            elif post_list:
                # Legacy format fallback: pageProps.posts[]
                for post in post_list:
                    post_id = post.get('id') or post.get('postId')
                    if post_id and post_id not in seen_ids:
                        seen_ids.add(post_id)
                        author = post.get('author', {}) or {}
                        author_name = post.get('authorName') or author.get('name', '')
                        posts.append({
                            'id': post_id,
                            'title': post.get('title', ''),
                            'content': post.get('content', '') or post.get('body', ''),
                            'authorName': author_name,
                            'author': author,
                            'url': post.get('url') or post.get('postUrl') or f"https://www.skool.com/{community}/{post_id}",
                            'postUrl': post.get('url') or post.get('postUrl') or f"https://www.skool.com/{community}/{post_id}",
                            'likesCount': post.get('likesCount', 0),
                            'commentsCount': post.get('commentsCount', 0),
                            'createdAt': post.get('createdAt', ''),
                            'categoryName': post.get('categoryName', ''),
                        })

                print(f"    Found {len(post_list)} posts ({len(posts)} total unique)")

            else:
                print(f"    No posts found in pageProps")

        if page_num < max_pages:
            await asyncio.sleep(5)

    return posts


async def scrape_recent_posts(community: str, max_pages: int = 2,
                               headless: bool = True) -> list:
    """Scrape recent posts (standalone browser)."""
    session = BrowserSession(headless=headless)
    await session.start()
    try:
        return await scrape_posts_with_page(session.page, community, max_pages)
    finally:
        await session.stop()


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def load_state(state_type: str, community: str) -> dict:
    path = STATE_DIR / f"{state_type}_{community}.json"
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"seen_ids": [], "last_run": None}


def save_state(state_type: str, community: str, state: dict):
    path = STATE_DIR / f"{state_type}_{community}.json"
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    tmp = path.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


def filter_new_ids(items: list, state: dict, id_key: str = "handle") -> list:
    seen = set(state.get("seen_ids", []))
    new_items = []
    for item in items:
        item_id = item.get(id_key, "").lower() if id_key == "handle" else str(item.get(id_key, ""))
        if item_id and item_id not in seen:
            new_items.append(item)
    return new_items


def add_to_state(state: dict, ids: list):
    seen = set(state.get("seen_ids", []))
    seen.update(ids)
    state["seen_ids"] = list(seen)


def load_enrichment_cache(community: str) -> dict:
    """Load cached enrichment data keyed by member handle."""
    path = STATE_DIR / f"enrichment_cache_{community}.json"
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_enrichment_cache(community: str, cache: dict):
    """Save enrichment cache atomically."""
    path = STATE_DIR / f"enrichment_cache_{community}.json"
    tmp = path.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


# ============================================================================
# EVENT LOGGING — daily JSONL for digest consumption
# ============================================================================

def log_event(community: str, event_type: str, data: dict):
    """Append event to daily JSONL log. Read by the nightly digest."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_path = EVENTS_DIR / f"{community}_{today}.jsonl"

    event = {
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "community": community,
        "data": data,
    }

    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


# ============================================================================
# NOTIFICATIONS — formatted with clickable links
# ============================================================================

def send_apprise_notification(title: str, body: str, notify_type: str = "info",
                               tag: str = None, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"\n  [DRY RUN] Notification ({notify_type}):")
        print(f"    Title: {title}")
        print(f"    Body: {body[:300]}...")
        return True

    urls = build_apprise_urls()
    if not urls:
        print("  WARNING: No Apprise notification URLs configured")
        return False

    payload = {
        "urls": ",".join(urls),
        "title": title,
        "body": body,
        "type": notify_type,
    }
    if tag:
        payload["tag"] = tag

    try:
        resp = requests.post(f"{APPRISE_URL}/notify", json=payload, timeout=30)
        if resp.status_code == 200:
            print(f"  Notification sent: {title}")
            return True
        else:
            print(f"  Notification failed ({resp.status_code}): {resp.text[:200]}")
            return False
    except requests.RequestException as e:
        print(f"  Notification error: {e}")
        return False


def format_member_notification(member: dict, enrichment: dict) -> tuple:
    """Format a single enriched ICP member into notification title + body with links."""
    tier = member.get("tier", "?")
    score = member.get("icp_score", 0)
    name = member.get("name", "Unknown")

    company = enrichment.get("company", "")
    desc = (enrichment.get("company_description", "") or "")[:140]
    services = ", ".join(enrichment.get("services", []))
    industries = ", ".join(enrichment.get("industries", []))
    city = enrichment.get("city", "")
    country = enrichment.get("country", "")
    location = f"{city}, {country}".strip(", ") if (city or country) else ""
    linkedin = enrichment.get("linkedin") or enrichment.get("linkedin_url", "")
    website = enrichment.get("website", "")

    title = f"NEW ICP [{tier}]: {name} (Score: {score})"

    lines = []
    # Company + what they do
    if company and desc:
        lines.append(f"{company} — {desc}")
    elif company:
        lines.append(company)
    elif desc:
        lines.append(desc)
    elif member.get("bio"):
        lines.append(member["bio"][:140])

    # Services / industries
    if services:
        lines.append(f"Services: {services}")
    if industries and not services:
        lines.append(f"Industry: {industries}")

    # Location
    if location:
        lines.append(f"Location: {location}")

    # Why flagged
    reasons = ", ".join(member.get("match_reasons", []))
    if reasons:
        lines.append(f"Signals: {reasons}")

    lines.append("")
    lines.append(f"Skool: {member.get('profileUrl', '')}")
    if linkedin:
        lines.append(f"LinkedIn: {linkedin}")
    if website:
        lines.append(f"Website: {website}")

    body = "\n".join(lines)
    return title, body


def format_churn_notification(member_data: dict) -> tuple:
    """Format a paid ICP cancellation notification with enrichment links."""
    name = member_data.get("name", "Unknown")
    handle = member_data.get("handle", "")
    tier = member_data.get("tier", "unknown")
    score = member_data.get("icp_score", 0)
    enrichment = member_data.get("enrichment", {})
    bio = member_data.get("bio", "")

    title = f"CANCELLED [{tier}]: {name} — $77/mo (Score: {score})"

    lines = []
    company = enrichment.get("company", "")
    desc = (enrichment.get("company_description", "") or "")[:140]
    services = ", ".join(enrichment.get("services", []))
    city = enrichment.get("city", "")
    country = enrichment.get("country", "")
    location = f"{city}, {country}".strip(", ") if (city or country) else ""
    linkedin = enrichment.get("linkedin") or enrichment.get("linkedin_url", "")
    website = enrichment.get("website", "")

    if company and desc:
        lines.append(f"{company} — {desc}")
    elif company:
        lines.append(company)
    elif bio:
        lines.append(bio[:140])

    if services:
        lines.append(f"Services: {services}")
    if location:
        lines.append(f"Location: {location}")

    joined = member_data.get("joinedAt", "")
    if joined:
        lines.append(f"Joined: {joined[:10]}")

    reasons = ", ".join(member_data.get("match_reasons", []))
    if reasons:
        lines.append(f"Signals: {reasons}")

    lines.append("")
    lines.append(f"Skool: https://www.skool.com/@{handle}")
    if linkedin:
        lines.append(f"LinkedIn: {linkedin}")
    if website:
        lines.append(f"Website: {website}")

    body = "\n".join(lines)
    return title, body


def format_wins_notification(wins: list) -> tuple:
    count = len(wins)
    title = f"Skool: {count} financial win{'s' if count != 1 else ''} posted"
    lines = []
    for w in wins:
        lines.append(f"{w['money_pattern'].upper()} — {w['author_name']}")
        if w.get('title'):
            lines.append(f'    "{w["title"][:100]}"')
        lines.append(f"    Post: {w.get('post_url', '')}")
        lines.append(f"    {w['likes_count']} likes | {w['comments_count']} comments")
        lines.append("")
    return title, "\n".join(lines)


def format_mentions_notification(mentions: list) -> tuple:
    count = len(mentions)
    title = f"Skool: {count} meaningful mention{'s' if count != 1 else ''}"
    lines = []
    for m in mentions:
        lines.append(f"[{m.get('type', 'mention')}] {m['author_name']}")
        if m.get('context'):
            lines.append(f'    "{m["context"][:150]}"')
        lines.append(f"    Post: {m.get('post_url', '')}")
        lines.append("")
    return title, "\n".join(lines)


def format_antigravity_notification(mentions: list) -> tuple:
    count = len(mentions)
    title = f"Skool: {count} anti-gravity mention{'s' if count != 1 else ''}!"
    lines = []
    for m in mentions:
        lines.append(f"{m['author_name']} mentioned \"{m['keyword']}\"")
        if m.get('post_title'):
            lines.append(f'    Post: "{m["post_title"][:80]}"')
        lines.append(f"    {m.get('post_url', '')}")
        lines.append("")
    return title, "\n".join(lines)


# ============================================================================
# CHURN DETECTION — removed, replaced by Cancelling tab scraper above
# ============================================================================
# Old approach diffed active member handles. New approach scrapes the
# Cancelling tab directly at /-/members?tab=cancelling which shows:
# - "Cancelled (churns in X days)" = paid cancellation → ALERT
# - "Trial cancelled (removing in X day)" = trial → skip
# See: scrape_cancelling_with_page() and detect_new_cancellations()


# ============================================================================
# TEST MODE — fire all 5 notification types with realistic fake data
# ============================================================================

def run_test_notifications(dry_run: bool = False):
    """
    Send one test notification for each type. No scraping — tests the full
    format → send pipeline with realistic data.

    Usage: python skool_apprise_monitor.py --test
           python skool_apprise_monitor.py --test --dry-run
    """
    print(f"\n{'='*60}")
    print("TEST MODE — Sending all 5 notification types")
    print(f"{'='*60}")

    sent = 0
    total = 5

    # --- 1. New ICP Member (Tier A) ---
    print("\n[1/5] New ICP Member (Tier A)")
    test_member = {
        "name": "Sarah Mitchell",
        "handle": "sarah-mitchell-test",
        "bio": "CEO at GrowthStack Digital. Helping local businesses scale with AI automation and GoHighLevel. Former agency owner, now building SaaS.",
        "profileUrl": "https://www.skool.com/@sarah-mitchell-test",
        "tier": "A",
        "icp_score": 72,
        "match_reasons": ["Position: ceo", "Industry: local business", "Pain: scale"],
    }
    test_enrichment = {
        "company": "GrowthStack Digital",
        "company_description": "AI-powered marketing automation for local service businesses. Specializes in GoHighLevel implementations.",
        "services": ["GoHighLevel", "AI Chatbots", "Marketing Automation"],
        "industries": ["Local Services", "Digital Marketing"],
        "city": "Austin",
        "country": "USA",
        "linkedin_url": "https://linkedin.com/in/sarahmitchell-test",
        "website": "https://growthstackdigital.com",
    }
    title, body = format_member_notification(test_member, test_enrichment)
    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
        sent += 1

    # --- 2. ICP Churn (Tier A cancelled) ---
    print("\n[2/5] ICP Churn (Tier A Cancellation)")
    test_churn = {
        "name": "Marcus Rivera",
        "handle": "marcus-rivera-test",
        "bio": "Owner of Rivera Home Services. 15 trucks, $2M revenue. Looking for better lead gen.",
        "tier": "A",
        "icp_score": 65,
        "match_reasons": ["Position: owner", "Industry: home services", "Revenue: $2m"],
        "joinedAt": "2025-11-15T00:00:00Z",
        "enrichment": {
            "company": "Rivera Home Services",
            "company_description": "Full-service HVAC and plumbing company serving the greater Phoenix area.",
            "services": ["HVAC", "Plumbing", "Emergency Repairs"],
            "city": "Phoenix",
            "country": "USA",
            "linkedin_url": "https://linkedin.com/in/marcusrivera-test",
            "website": "https://riverahomeservices.com",
        },
    }
    title, body = format_churn_notification(test_churn)
    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
        sent += 1

    # --- 3. Financial Win ---
    print("\n[3/5] Financial Win")
    test_wins = [{
        "money_pattern": "$15,000 deal",
        "author_name": "Jake Thompson",
        "author_handle": "jake-thompson-test",
        "title": "Just closed my biggest client ever — $15,000/month retainer for AI automation!",
        "post_url": "https://www.skool.com/aiautomationsbyjack/test-post-123",
        "likes_count": 47,
        "comments_count": 23,
    }]
    title, body = format_wins_notification(test_wins)
    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
        sent += 1

    # --- 4. Meaningful @florian Mention ---
    print("\n[4/5] Meaningful @florian Mention")
    test_mentions = [{
        "type": "@mention",
        "author_name": "David Park",
        "author_handle": "david-park-test",
        "context": "...has anyone tried building a GoHighLevel integration with AI agents? @florian I saw your post about automation workflows — would love to hear how you approached the appointment booking pipeline...",
        "post_url": "https://www.skool.com/aiautomationsbyjack/test-mention-456",
        "meaningful": True,
    }]
    title, body = format_mentions_notification(test_mentions)
    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
        sent += 1

    # --- 5. Anti-Gravity Brand Mention ---
    print("\n[5/5] Anti-Gravity Brand Mention")
    test_ag = [{
        "author_name": "Lisa Chen",
        "author_handle": "lisa-chen-test",
        "keyword": "anti-gravity",
        "post_title": "Tools and resources that actually helped me scale",
        "post_url": "https://www.skool.com/aiautomationsbyjack/test-ag-789",
    }]
    title, body = format_antigravity_notification(test_ag)
    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
        sent += 1

    print(f"\n{'='*60}")
    print(f"TEST COMPLETE: {sent}/{total} notifications sent")
    print(f"{'='*60}")
    return sent


# ============================================================================
# MAIN MONITOR
# ============================================================================

async def run_monitor(community: str, headless: bool = True,
                       dry_run: bool = False, init: bool = False,
                       members_only: bool = False, posts_only: bool = False,
                       session: BrowserSession = None):
    """
    Run the unified Skool monitor (single cycle).

    1. Scrape members → detect new ICPs → enrich → notify with links
    2. Detect churn (cancelled members) → alert for ICP qualified
    3. Scrape posts → detect wins + mentions + antigravity → notify
    """
    print(f"\n{'='*60}")
    print(f"SKOOL MONITOR v2 — {community}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    results = {
        "new_members": 0, "enriched": 0, "churned": 0,
        "wins": 0, "mentions": 0, "antigravity": 0,
        "notifications_sent": 0,
    }

    enrichment_cache = load_enrichment_cache(community)

    # --- MEMBER MONITORING ---
    if not posts_only:
        print(f"\n[1/3] MEMBER MONITORING")
        member_state = load_state("members", community)

        try:
            if session and session.is_alive:
                scraped = await scrape_members_with_page(session.page, community, max_pages=1)
            else:
                scraped = await scrape_member_list(community, max_pages=2, headless=headless)
            print(f"  Scraped {len(scraped)} members")

            current_handles = {m["handle"].lower() for m in scraped if m.get("handle")}

            if init and not member_state.get("seen_ids"):
                # First run: initialize state, no notifications
                add_to_state(member_state, list(current_handles))
                save_state("members", community, member_state)
                print(f"  Initialized state with {len(current_handles)} members.")
            else:
                # --- NEW MEMBER DETECTION ---
                new_members = filter_new_ids(scraped, member_state, id_key="handle")
                print(f"\n  New members: {len(new_members)}")

                if new_members:
                    # Score all new members first (quick keyword scoring)
                    scored = [quick_score_member(m) for m in new_members]
                    qualified = [m for m in scored if m.get("tier") in ("A", "B")]

                    tier_counts = {}
                    for m in scored:
                        t = m.get("tier", "?")
                        tier_counts[t] = tier_counts.get(t, 0) + 1
                    print(f"  Tiers: {tier_counts}")

                    # Enrich qualified members BEFORE notifying
                    for m in qualified:
                        handle = m["handle"].lower()
                        enrichment = enrich_member(m)
                        results["enriched"] += 1

                        # Re-score with enrichment data for better accuracy
                        m = quick_score_member(m, enrichment)

                        # Cache enrichment data (for churn detection + digest)
                        enrichment_cache[handle] = {
                            "name": m.get("name", ""),
                            "handle": handle,
                            "tier": m.get("tier", "D"),
                            "icp_score": m.get("icp_score", 0),
                            "match_reasons": m.get("match_reasons", []),
                            "enrichment": enrichment,
                            "cached_at": datetime.now(timezone.utc).isoformat(),
                        }

                        # Only notify if still qualified after re-scoring with enrichment
                        if m.get("tier") in ("A", "B"):
                            results["new_members"] += 1
                            title, body = format_member_notification(m, enrichment)
                            if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
                                results["notifications_sent"] += 1

                            # Log event for daily digest
                            log_event(community, "new_member", {
                                "handle": handle,
                                "name": m.get("name", ""),
                                "tier": m.get("tier"),
                                "icp_score": m.get("icp_score"),
                                "match_reasons": m.get("match_reasons", []),
                                "enrichment": enrichment,
                            })

                    # Also cache basic data for non-qualified members (for churn tracking)
                    for m in scored:
                        handle = m["handle"].lower()
                        if handle not in enrichment_cache:
                            enrichment_cache[handle] = {
                                "name": m.get("name", ""),
                                "handle": handle,
                                "tier": m.get("tier", "D"),
                                "icp_score": m.get("icp_score", 0),
                                "cached_at": datetime.now(timezone.utc).isoformat(),
                            }

                    save_enrichment_cache(community, enrichment_cache)

                # Update member state with ALL current handles
                add_to_state(member_state, list(current_handles))

            save_state("members", community, member_state)

        except Exception as e:
            print(f"  ERROR in member monitoring: {e}")
            import traceback
            traceback.print_exc()

    # --- CANCELLATION MONITORING (scrape Cancelling tab) ---
    if not posts_only:
        print(f"\n[2/3] CANCELLATION MONITORING (Cancelling tab)")
        try:
            if session and session.is_alive:
                cancelling = await scrape_cancelling_with_page(session.page, community, max_pages=2)
            else:
                # Standalone mode — need a browser
                tmp_session = BrowserSession(headless=headless)
                await tmp_session.start()
                try:
                    cancelling = await scrape_cancelling_with_page(tmp_session.page, community, max_pages=2)
                finally:
                    await tmp_session.stop()

            print(f"  Total cancelling: {len(cancelling)}")
            trial_count = sum(1 for m in cancelling if m.get("is_trial"))
            paid_count = len(cancelling) - trial_count
            print(f"  Breakdown: {paid_count} paid, {trial_count} trial")

            if not init:
                new_cancellations = detect_new_cancellations(cancelling, community, enrichment_cache)
                if new_cancellations:
                    print(f"  NEW paid cancellations to evaluate: {len(new_cancellations)}")
                    notified = 0

                    for cancel in new_cancellations:
                        handle = cancel["handle"]

                        # Enrich cancelling member (Perplexity + Tavily LinkedIn)
                        if not cancel.get("enrichment") or not cancel["enrichment"].get("company"):
                            enrichment = enrich_member(cancel)
                            cancel["enrichment"] = enrichment
                        else:
                            enrichment = cancel["enrichment"]

                        # Re-score WITH enrichment data now available
                        cancel = quick_score_member(cancel, enrichment)

                        # ICP FILTER: only notify for Tier A or B members
                        tier = cancel.get("tier", "D")
                        if tier not in ("A", "B"):
                            print(f"    Skip (not ICP, tier {tier}): {cancel['name']}")
                            log_event(community, "cancellation", cancel)  # Log for digest
                            continue

                        # Update enrichment cache with full data
                        enrichment_cache[handle] = {
                            "name": cancel.get("name", ""),
                            "handle": handle,
                            "tier": tier,
                            "icp_score": cancel.get("icp_score", 0),
                            "enrichment": enrichment,
                            "cached_at": datetime.now(timezone.utc).isoformat(),
                        }

                        title, body = format_churn_notification(cancel)
                        # Use "info" — silent visual popup, no sound
                        if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
                            notified += 1
                            results["notifications_sent"] += 1
                        log_event(community, "cancellation", cancel)

                    results["churned"] = notified
                    if notified:
                        print(f"  ICP cancellation alerts sent: {notified}")
                    save_enrichment_cache(community, enrichment_cache)
                else:
                    print(f"  No new paid cancellations")
            else:
                # Init mode (--init flag): handled in detect_new_cancellations on first run
                print(f"  Cancelling tab: {len(cancelling)} members (init mode, no alerts)")

        except Exception as e:
            print(f"  ERROR in cancellation monitoring: {e}")
            import traceback
            traceback.print_exc()

    # --- POST MONITORING (wins + mentions + antigravity) ---
    if not members_only:
        print(f"\n[3/3] POST MONITORING")
        post_state = load_state("posts", community)

        try:
            if session and session.is_alive:
                posts = await scrape_posts_with_page(session.page, community, max_pages=2)
            else:
                posts = await scrape_recent_posts(community, max_pages=2, headless=headless)
            print(f"  Scraped {len(posts)} posts")

            new_posts = filter_new_ids(posts, post_state, id_key="id")
            print(f"  New posts: {len(new_posts)}")

            if init and not post_state.get("seen_ids"):
                post_ids = [str(p.get("id", "")) for p in posts if p.get("id")]
                add_to_state(post_state, post_ids)
                save_state("posts", community, post_state)
                print(f"  Initialized state with {len(post_ids)} posts.")
            elif new_posts:
                # Financial wins
                wins = detect_wins(new_posts)
                if wins:
                    results["wins"] = len(wins)
                    title, body = format_wins_notification(wins)
                    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
                        results["notifications_sent"] += 1
                    print(f"  Financial wins: {len(wins)}")
                    for w in wins:
                        log_event(community, "win", w)

                # Anti-gravity / brand mentions
                ag_mentions = detect_antigravity_mentions(new_posts)
                if ag_mentions:
                    results["antigravity"] = len(ag_mentions)
                    title, body = format_antigravity_notification(ag_mentions)
                    if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
                        results["notifications_sent"] += 1
                    print(f"  Anti-gravity mentions: {len(ag_mentions)}")
                    for ag in ag_mentions:
                        log_event(community, "antigravity", ag)

                # @florian mentions — only notify if meaningful
                mentions = detect_mentions(new_posts)
                if mentions:
                    meaningful = [m for m in mentions if m.get("meaningful")]
                    noise = [m for m in mentions if not m.get("meaningful")]

                    if meaningful:
                        results["mentions"] = len(meaningful)
                        title, body = format_mentions_notification(meaningful)
                        if send_apprise_notification(title, body, notify_type="info", dry_run=dry_run):
                            results["notifications_sent"] += 1
                        print(f"  Meaningful mentions: {len(meaningful)}")

                    if noise:
                        print(f"  Filtered noise mentions: {len(noise)} (logged only)")

                    # Log ALL mentions (meaningful and noise) for daily digest
                    for m in mentions:
                        log_event(community, "mention", m)

                post_ids = [str(p.get("id", "")) for p in posts if p.get("id")]
                add_to_state(post_state, post_ids)

            save_state("posts", community, post_state)

        except Exception as e:
            print(f"  ERROR in post monitoring: {e}")
            import traceback
            traceback.print_exc()

    # --- SUMMARY ---
    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  New ICP members:    {results['new_members']} (enriched: {results['enriched']})")
    print(f"  Churn (ICP):        {results['churned']}")
    print(f"  Financial wins:     {results['wins']}")
    print(f"  Mentions:           {results['mentions']}")
    print(f"  Anti-gravity:       {results['antigravity']}")
    print(f"  Notifications sent: {results['notifications_sent']}")
    print(f"{'='*60}\n")

    return results


# ============================================================================
# DAEMON MODE
# ============================================================================

def _check_and_run_digest(community: str, dry_run: bool = False):
    """
    Check if it's time to send the daily digest (9:30pm EST / 02:30 UTC).

    Runs once per day. Uses a state file to track whether today's digest was sent.
    The digest window is 02:25-02:35 UTC (9:25-9:35pm EST) to handle cycle timing.
    """
    from datetime import timedelta

    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")

    # Check if digest already sent today
    digest_state_path = STATE_DIR / f"digest_sent_{community}.json"
    if digest_state_path.exists():
        try:
            with open(digest_state_path, 'r') as f:
                state = json.load(f)
            if state.get("last_sent") == today_str:
                return  # Already sent today
        except (json.JSONDecodeError, KeyError):
            pass

    # Check if we're in the digest window: 02:25-02:35 UTC (9:25-9:35pm EST)
    hour, minute = now_utc.hour, now_utc.minute
    in_window = (hour == 2 and 25 <= minute <= 35)

    # Also support configurable hour via env var (DIGEST_HOUR_UTC, default 2 = 9pm EST)
    digest_hour = int(os.getenv("DIGEST_HOUR_UTC", "2"))
    if digest_hour != 2:
        in_window = (hour == digest_hour and 25 <= minute <= 35)

    if not in_window:
        return

    print(f"\n  [DIGEST] 9:30pm EST window detected — generating daily digest...")

    try:
        # Import the digest module (in Docker: same /app directory)
        try:
            from skool_daily_digest_v3 import run_digest
        except ImportError:
            # Try from execution/ directory (local development)
            sys.path.insert(0, str(Path(__file__).parent))
            from skool_daily_digest_v3 import run_digest

        email_to = os.getenv("APPRISE_EMAIL_TO", "florian@florianrolke.com")
        success = run_digest(
            community=community,
            dry_run=dry_run,
            email_to=email_to,
        )

        if success:
            # Mark digest as sent today
            with open(digest_state_path, 'w') as f:
                json.dump({"last_sent": today_str, "sent_at": now_utc.isoformat()}, f)
            print(f"  [DIGEST] Sent successfully. Next digest tomorrow.")
        else:
            print(f"  [DIGEST] No events today or send failed.")

    except Exception as e:
        print(f"  [DIGEST] Error: {e}")
        import traceback
        traceback.print_exc()


async def run_daemon(community: str, interval: int = 180, headless: bool = True,
                      dry_run: bool = False, members_only: bool = False,
                      posts_only: bool = False):
    """Run the monitor in a persistent loop (daemon mode)."""
    print(f"\n{'='*60}")
    print(f"SKOOL MONITOR v2 — DAEMON MODE")
    print(f"Community: {community}")
    print(f"Interval: {interval}s ({interval/60:.1f} min)")
    print(f"Enrichment: {'enabled' if PERPLEXITY_KEY else 'DISABLED (no API key)'}")
    print(f"LinkedIn finder: {'enabled' if TAVILY_KEYS else 'DISABLED (no API keys)'}")
    print(f"Digest: 9:30pm EST daily (02:30 UTC)")
    print(f"Dry run: {dry_run}")
    print(f"{'='*60}\n")

    session = BrowserSession(headless=headless)
    cycle = 0

    member_state_path = STATE_DIR / f"members_{community}.json"
    needs_init = not member_state_path.exists()
    if needs_init:
        print("  No state file found — first cycle will initialize (no notifications).")

    try:
        await session.start()

        while True:
            cycle += 1
            start_time = datetime.now()
            print(f"\n--- Cycle {cycle} | {start_time.strftime('%H:%M:%S')} ---")

            try:
                await run_monitor(
                    community=community,
                    headless=headless,
                    dry_run=dry_run,
                    members_only=members_only,
                    posts_only=posts_only,
                    session=session,
                    init=needs_init,
                )
                if needs_init:
                    needs_init = False
                    print("  State initialized. Next cycles will detect changes.")
            except Exception as e:
                print(f"  CYCLE ERROR: {e}")
                import traceback
                traceback.print_exc()
                try:
                    await session.stop()
                except Exception:
                    pass
                await asyncio.sleep(10)
                await session.start()

            # Check if it's time for the daily digest (9:30pm EST)
            _check_and_run_digest(community, dry_run=dry_run)

            await session.maybe_restart()

            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                print(f"  Sleeping {sleep_time:.0f}s until next cycle...")
                await asyncio.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\nDaemon stopped by user (Ctrl+C)")
    finally:
        await session.stop()
        print("Browser closed. Daemon exited.")


async def main():
    parser = argparse.ArgumentParser(description="Skool Apprise Monitor v2")
    parser.add_argument("--community", default=COMMUNITY,
                        help=f"Community slug (default: {COMMUNITY})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print notifications but don't send")
    parser.add_argument("--init", action="store_true",
                        help="Initialize state files (first run, no notifications)")
    parser.add_argument("--visible", action="store_true",
                        help="Show browser window for debugging")
    parser.add_argument("--members-only", action="store_true",
                        help="Only monitor members, skip posts")
    parser.add_argument("--posts-only", action="store_true",
                        help="Only monitor posts, skip members")
    parser.add_argument("--daemon", action="store_true",
                        help="Run in persistent loop (daemon mode)")
    parser.add_argument("--interval", type=int, default=180,
                        help="Seconds between checks in daemon mode (default: 180)")
    parser.add_argument("--test", action="store_true",
                        help="Send test notifications for all 5 alert types (no scraping)")
    args = parser.parse_args()

    if args.test:
        run_test_notifications(dry_run=args.dry_run)
        return

    if args.daemon:
        await run_daemon(
            community=args.community,
            interval=args.interval,
            headless=not args.visible,
            dry_run=args.dry_run,
            members_only=args.members_only,
            posts_only=args.posts_only,
        )
    else:
        await run_monitor(
            community=args.community,
            headless=not args.visible,
            dry_run=args.dry_run,
            init=args.init,
            members_only=args.members_only,
            posts_only=args.posts_only,
        )


if __name__ == "__main__":
    asyncio.run(main())
