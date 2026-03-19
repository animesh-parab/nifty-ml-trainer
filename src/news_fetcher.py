"""
news_fetcher.py
Standalone news fetcher — Moneycontrol RSS + Economic Times RSS + NewsAPI
Fetches headlines, scores sentiment via Groq, saves to data/processed/news_log.json
Run standalone to verify: python src/news_fetcher.py
"""

import os
import json
import requests
import feedparser
from datetime import datetime, timezone
from dotenv import load_dotenv
import groq

load_dotenv()

GROQ_KEY    = os.getenv("GROQ_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
NEWS_LOG_PATH = "data/processed/news_log.json"

# ── RSS Sources ──────────────────────────────────────────────
RSS_FEEDS = {
    "moneycontrol": "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "economic_times": "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
}

# ── NewsAPI config ───────────────────────────────────────────
NEWSAPI_URL = "https://newsapi.org/v2/everything"
NEWSAPI_PARAMS = {
    "q":        "Nifty OR \"Indian market\" OR Sensex OR NSE OR RBI",
    "language": "en",
    "sortBy":   "publishedAt",
    "pageSize": 10,
    "apiKey":   NEWS_API_KEY,
}


# ── Fetch RSS headlines ──────────────────────────────────────
def fetch_rss(name, url):
    print(f"  Fetching {name}...")
    try:
        feed = feedparser.parse(url)
        headlines = []
        for entry in feed.entries[:10]:
            headlines.append({
                "source":    name,
                "title":     entry.get("title", "").strip(),
                "published": entry.get("published", ""),
                "link":      entry.get("link", ""),
            })
        print(f"  ✓ {name}: {len(headlines)} headlines")
        return headlines
    except Exception as e:
        print(f"  ✗ {name} error: {e}")
        return []


# ── Fetch NewsAPI headlines ──────────────────────────────────
def fetch_newsapi():
    print("  Fetching NewsAPI...")
    try:
        response = requests.get(NEWSAPI_URL, params=NEWSAPI_PARAMS, timeout=10)
        data = response.json()
        if data.get("status") != "ok":
            print(f"  ✗ NewsAPI error: {data.get('message')}")
            return []
        headlines = []
        for article in data.get("articles", []):
            headlines.append({
                "source":    "newsapi_" + (article.get("source", {}).get("name", "unknown")),
                "title":     article.get("title", "").strip(),
                "published": article.get("publishedAt", ""),
                "link":      article.get("url", ""),
            })
        print(f"  ✓ NewsAPI: {len(headlines)} headlines")
        return headlines
    except Exception as e:
        print(f"  ✗ NewsAPI error: {e}")
        return []


# ── Score sentiment via Groq ─────────────────────────────────
def score_sentiment(headlines):
    if not headlines or not GROQ_KEY:
        return headlines

    print(f"\n  Scoring sentiment via Groq ({len(headlines)} headlines)...")
    client = groq.Groq(api_key=GROQ_KEY)

    headline_text = "\n".join([
        f"{i+1}. {h['title']}" for i, h in enumerate(headlines)
    ])

    prompt = f"""You are a financial sentiment analyst for Indian stock markets.

Analyze these news headlines and score each one for its likely impact on Nifty 50 index.

Headlines:
{headline_text}

For each headline, respond with ONLY a JSON array in this exact format:
[
  {{"index": 1, "sentiment": "BULLISH", "score": 0.8, "reason": "one short reason"}},
  {{"index": 2, "sentiment": "BEARISH", "score": -0.6, "reason": "one short reason"}},
  ...
]

Rules:
- sentiment: BULLISH, BEARISH, or NEUTRAL
- score: float from -1.0 (very bearish) to +1.0 (very bullish), 0.0 for neutral
- reason: max 8 words
- Return ONLY the JSON array, no other text"""

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000
        )
        raw = response.choices[0].message.content.strip()

        # Clean JSON if needed
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        scores = json.loads(raw)
        score_map = {item["index"]: item for item in scores}

        for i, h in enumerate(headlines):
            s = score_map.get(i + 1, {})
            h["sentiment"] = s.get("sentiment", "NEUTRAL")
            h["score"]     = s.get("score", 0.0)
            h["reason"]    = s.get("reason", "")

        print(f"  ✓ Sentiment scored")
        return headlines

    except Exception as e:
        print(f"  ✗ Groq sentiment error: {e}")
        for h in headlines:
            h["sentiment"] = "NEUTRAL"
            h["score"]     = 0.0
            h["reason"]    = ""
        return headlines


# ── Calculate overall market sentiment ──────────────────────
def calculate_overall(headlines):
    scored = [h for h in headlines if "score" in h]
    if not scored:
        return {"sentiment": "NEUTRAL", "score": 0.0, "total_headlines": 0}

    avg_score = sum(h["score"] for h in scored) / len(scored)
    bullish   = sum(1 for h in scored if h.get("sentiment") == "BULLISH")
    bearish   = sum(1 for h in scored if h.get("sentiment") == "BEARISH")
    neutral   = sum(1 for h in scored if h.get("sentiment") == "NEUTRAL")

    if avg_score > 0.2:
        overall = "BULLISH"
    elif avg_score < -0.2:
        overall = "BEARISH"
    else:
        overall = "NEUTRAL"

    return {
        "sentiment":       overall,
        "score":           round(avg_score, 3),
        "bullish_count":   bullish,
        "bearish_count":   bearish,
        "neutral_count":   neutral,
        "total_headlines": len(scored),
    }


# ── Save to JSON ─────────────────────────────────────────────
def save_news_log(headlines, overall):
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall":   overall,
        "headlines": headlines,
    }

    # Load existing log
    existing = []
    if os.path.exists(NEWS_LOG_PATH):
        try:
            with open(NEWS_LOG_PATH, "r") as f:
                existing = json.load(f)
        except:
            existing = []

    # Append new entry
    existing.append(log)

    # Keep last 50 fetch cycles only
    if len(existing) > 50:
        existing = existing[-50:]

    with open(NEWS_LOG_PATH, "w") as f:
        json.dump(existing, f, indent=2)

    print(f"\n  ✓ Saved to {NEWS_LOG_PATH}")


# ── Print summary ────────────────────────────────────────────
def print_summary(headlines, overall):
    print("\n" + "=" * 60)
    print("NEWS SENTIMENT SUMMARY")
    print("=" * 60)

    color_map = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}

    print(f"\nOVERALL: {color_map.get(overall['sentiment'], '⚪')} {overall['sentiment']} "
          f"(score: {overall['score']:+.3f})")
    print(f"Headlines: {overall['bullish_count']} bullish | "
          f"{overall['bearish_count']} bearish | "
          f"{overall['neutral_count']} neutral")

    print(f"\n{'─' * 60}")
    print("TOP HEADLINES:")
    print(f"{'─' * 60}")

    # Sort by absolute score (most impactful first)
    sorted_headlines = sorted(headlines, key=lambda h: abs(h.get("score", 0)), reverse=True)

    for h in sorted_headlines[:10]:
        icon = color_map.get(h.get("sentiment", "NEUTRAL"), "⚪")
        score = h.get("score", 0.0)
        print(f"\n{icon} [{h['source']}] {score:+.2f}")
        print(f"   {h['title'][:100]}")
        if h.get("reason"):
            print(f"   → {h['reason']}")

    print("\n" + "=" * 60)


# ── Main ─────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Nifty ML Trainer — News Fetcher")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print("\nFetching headlines...")
    headlines = []

    # RSS feeds
    for name, url in RSS_FEEDS.items():
        headlines.extend(fetch_rss(name, url))

    # NewsAPI
    if NEWS_API_KEY:
        headlines.extend(fetch_newsapi())
    else:
        print("  ⚠ NEWS_API_KEY not set — skipping NewsAPI")

    print(f"\nTotal headlines fetched: {len(headlines)}")

    if not headlines:
        print("✗ No headlines fetched. Check internet connection.")
        return

    # Score sentiment
    headlines = score_sentiment(headlines)

    # Overall sentiment
    overall = calculate_overall(headlines)

    # Print summary
    print_summary(headlines, overall)

    # Save
    save_news_log(headlines, overall)


if __name__ == "__main__":
    main()
