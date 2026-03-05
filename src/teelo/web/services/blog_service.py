from datetime import datetime
from pathlib import Path
from typing import Any

import frontmatter
import markdown


def get_blog_posts(content_path: Path) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    blog_dir = content_path / "blog"
    if not blog_dir.exists():
        return posts
    for file_path in blog_dir.glob("*.md"):
        post = frontmatter.load(file_path)
        if post.get("draft", False):
            continue
        posts.append(
            {
                "slug": file_path.stem,
                "title": post.get("title", "Untitled"),
                "date": post.get("date", datetime.min),
                "author": post.get("author", "Unknown"),
                "excerpt": post.get("excerpt", ""),
                "category": post.get("category", "Blog"),
                "content": markdown.markdown(post.content, extensions=["tables"]),
            }
        )
    return sorted(posts, key=lambda x: x["date"], reverse=True)


def get_blog_post(content_path: Path, slug: str) -> dict[str, Any] | None:
    file_path = content_path / "blog" / f"{slug}.md"
    if not file_path.exists():
        return None
    post = frontmatter.load(file_path)
    if post.get("draft", False):
        return None
    return {
        "slug": slug,
        "title": post.get("title", "Untitled"),
        "date": post.get("date", datetime.min),
        "author": post.get("author", "Unknown"),
        "category": post.get("category", "Blog"),
        "content": markdown.markdown(post.content, extensions=["tables"]),
    }
