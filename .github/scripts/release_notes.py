#!/usr/bin/env python3
"""Create GitHub releases with generated notes and preserve published reruns."""

import json
import os
import re
import sys
from urllib.parse import quote
from urllib.request import Request, urlopen

VERSION = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")


class GitHub:
    def __init__(self):
        self.root = os.environ["GITHUB_API_URL"] + "/repos/" + os.environ["GITHUB_REPOSITORY"]
        self.token = os.environ["GH_TOKEN"]

    def request(self, method, path, data=None):
        request = Request(
            self.root + path,
            data=json.dumps(data).encode() if data is not None else None,
            method=method,
            headers={"Authorization": "Bearer " + self.token,
                     "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28",
                     "Content-Type": "application/json"},
        )
        with urlopen(request, timeout=60) as response:
            body = response.read()
            return json.loads(body) if body else None


def create_release(api, tag, commit):
    if not VERSION.fullmatch(tag):
        raise ValueError("Release tag must match vMAJOR.MINOR.PATCH")
    ref = api.request("GET", "/git/ref/tags/" + quote(tag, safe=""))["object"]
    while ref["type"] == "tag":
        ref = api.request("GET", "/git/tags/" + ref["sha"])["object"]
    if ref["type"] != "commit" or ref["sha"] != commit:
        raise ValueError("Release tag no longer points at the built commit")
    # The by-tag endpoint returns published releases only. Listing with this
    # workflow's contents-write token also exposes drafts, including older pages.
    page = 1
    while True:
        releases = api.request("GET", f"/releases?per_page=100&page={page}")
        if not isinstance(releases, list):
            raise ValueError("Invalid release listing")
        for existing in releases:
            if existing.get("tag_name") == tag:
                if existing.get("draft") is not False or not existing.get("published_at"):
                    raise ValueError("Existing release is unpublished; publish or remove it before retrying")
                return existing, False
        if not releases:
            break
        page += 1
    notes = api.request("POST", "/releases/generate-notes", {
        "tag_name": tag, "configuration_file_path": ".github/release.yml",
    })
    created = api.request("POST", "/releases", {
        "tag_name": tag, "name": tag, "body": notes["body"],
    })
    if (created.get("tag_name") != tag or created.get("draft") is not False
            or not created.get("published_at")):
        raise ValueError("Release creation did not return the requested published release")
    return created, True


if __name__ == "__main__":
    api = GitHub()
    if sys.argv[1] == "release":
        _, created = create_release(api, os.environ["RELEASE_TAG"], os.environ["BUILT_COMMIT"])
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"created={str(created).lower()}\n")
    else:
        raise ValueError("Expected release")
