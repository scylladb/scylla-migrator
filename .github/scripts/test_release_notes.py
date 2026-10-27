import importlib.util
import unittest
import json
import os
import subprocess
import sys
import threading
from tempfile import TemporaryDirectory
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, call

spec = importlib.util.spec_from_file_location("release_notes", Path(__file__).with_name("release_notes.py"))
notes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(notes)


class ReleaseTests(unittest.TestCase):
    def test_create_filtered_release_after_tag_verification(self):
        api = Mock()
        api.request.side_effect = [{"object": {"type": "commit", "sha": "abc"}}, [],
                                   {"body": "Filtered notes"}, {"id": 1, "tag_name": "v2.1.7",
                                    "draft": False, "published_at": "2026-10-25T15:00:00Z"}]
        self.assertEqual(notes.create_release(api, "v2.1.7", "abc")[1], True)
        self.assertEqual(api.request.call_args_list, [
            call("GET", "/git/ref/tags/v2.1.7"),
            call("GET", "/releases?per_page=100&page=1"),
            call("POST", "/releases/generate-notes", {"tag_name": "v2.1.7",
                 "configuration_file_path": ".github/release.yml"}),
            call("POST", "/releases", {"tag_name": "v2.1.7", "name": "v2.1.7", "body": "Filtered notes"}),
        ])

    def test_rerun_preserves_existing_release_and_curated_notes(self):
        api = Mock()
        api.request.side_effect = [{"object": {"type": "tag", "sha": "tagsha"}},
                                   {"object": {"type": "commit", "sha": "abc"}},
                                   [{"id": 1, "tag_name": "v2.1.7", "body": "Curated notes", "draft": False,
                                    "published_at": "2026-10-25T15:00:00Z"}]]
        release, created = notes.create_release(api, "v2.1.7", "abc")
        self.assertEqual(release["body"], "Curated notes")
        self.assertFalse(created)
        self.assertFalse(any(c.args[0] == "POST" for c in api.request.call_args_list))

    def test_existing_unpublished_release_blocks_artifact_and_docker_publication(self):
        for release in [
            {"id": 1, "draft": True, "published_at": None},
            {"id": 1, "draft": False, "published_at": None},
            {"id": 1, "draft": False},
            {"id": 1, "published_at": "2026-10-25T15:00:00Z"},
        ]:
            with self.subTest(release=release):
                api = Mock()
                api.request.side_effect = [
                    {"object": {"type": "commit", "sha": "abc"}}, [dict(release, tag_name="v2.1.7")],
                ]
                with self.assertRaisesRegex(ValueError, "unpublished"):
                    notes.create_release(api, "v2.1.7", "abc")
                self.assertEqual(api.request.call_count, 2)
                self.assertFalse(any(c.args[0] != "GET" for c in api.request.call_args_list))

    def test_moved_tag_fails_before_release_write(self):
        api = Mock()
        api.request.return_value = {"object": {"type": "commit", "sha": "different"}}
        with self.assertRaisesRegex(ValueError, "built commit"):
            notes.create_release(api, "v2.1.7", "abc")
        self.assertEqual(api.request.call_count, 1)

    def test_invalid_tags_fail_before_api(self):
        api = Mock()
        for tag in ["latest", "v2.1", "v2.1.7-rc1", "v2.1.7;echo bad"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                notes.create_release(api, tag, "abc")
        api.request.assert_not_called()


class ClientContractTests(unittest.TestCase):
    def test_real_client_and_cli_follow_release_api_contract(self):
        scenarios = ["absent", "draft-page-two", "published", "unauthorized", "listing-failure", "moved-tag", "create-conflict", "invalid-create-response"]
        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                calls = []

                class Handler(BaseHTTPRequestHandler):
                    def log_message(self, *args):
                        pass

                    def respond(self):
                        data = None
                        if self.command == "POST":
                            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                        calls.append((self.command, self.path, data, self.headers.get("Authorization")))
                        prefix = "/repos/scylladb/scylla-migrator"
                        path = self.path.removeprefix(prefix)
                        status = 200
                        if scenario == "unauthorized":
                            status, result = 401, {"message": "Bad credentials"}
                        elif path == "/git/ref/tags/v2.1.7":
                            result = {"object": {"type": "commit", "sha": "other" if scenario == "moved-tag" else "abc"}}
                        elif path == "/releases?per_page=100&page=1":
                            if scenario == "listing-failure":
                                status, result = 503, {"message": "Unavailable"}
                            elif scenario == "published":
                                result = [{"id": 1, "tag_name": "v2.1.7", "draft": False,
                                           "published_at": "2026-10-25T15:00:00Z", "body": "Curated notes"}]
                            elif scenario == "draft-page-two":
                                result = [{"id": 2, "tag_name": "v2.1.6", "draft": False}]
                            else:
                                result = []
                        elif path == "/releases?per_page=100&page=2" and scenario == "draft-page-two":
                            result = [{"id": 1, "tag_name": "v2.1.7", "draft": True, "published_at": None}]
                        elif path == "/releases/generate-notes" and self.command == "POST":
                            result = {"body": "Filtered notes"}
                        elif path == "/releases" and self.command == "POST":
                            if scenario == "create-conflict":
                                status, result = 422, {"message": "already_exists"}
                            else:
                                result = dict(data, id=1, draft=False, published_at="2026-10-25T15:00:00Z")
                                if scenario == "invalid-create-response":
                                    result["draft"] = True
                        else:
                            status, result = 404, {"message": "Unexpected endpoint"}
                        body = json.dumps(result).encode()
                        self.send_response(status)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)

                    do_GET = respond
                    do_POST = respond

                with TemporaryDirectory() as temporary, ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
                    thread = threading.Thread(target=server.serve_forever, daemon=True)
                    thread.start()
                    try:
                        env = dict(os.environ, GITHUB_API_URL=f"http://127.0.0.1:{server.server_port}",
                                   GITHUB_REPOSITORY="scylladb/scylla-migrator", GH_TOKEN="test-token",
                                   RELEASE_TAG="v2.1.7", BUILT_COMMIT="abc", PYTHONDONTWRITEBYTECODE="1",
                                   GITHUB_OUTPUT=str(Path(temporary) / "outputs"))
                        result = subprocess.run([sys.executable, str(Path(__file__).with_name("release_notes.py")),
                                                 "release"], env=env, capture_output=True, text=True, timeout=10)
                    finally:
                        server.shutdown()
                        thread.join()
                    output_path = Path(temporary) / "outputs"
                    outputs = output_path.read_text() if output_path.exists() else ""
                self.assertEqual(outputs, "created=true\n" if scenario == "absent" else
                                 "created=false\n" if scenario == "published" else "")
                self.assertTrue(all(c[3] == "Bearer test-token" for c in calls))
                writes = [(method, path, data) for method, path, data, _ in calls if method != "GET"]
                if scenario == "absent":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual([c[1] for c in writes], [
                        "/repos/scylladb/scylla-migrator/releases/generate-notes",
                        "/repos/scylladb/scylla-migrator/releases"])
                    self.assertEqual(writes[0][2], {"tag_name": "v2.1.7", "configuration_file_path": ".github/release.yml"})
                    self.assertEqual(writes[1][2]["body"], "Filtered notes")
                elif scenario in ["create-conflict", "invalid-create-response"]:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual([c[1] for c in writes], [
                        "/repos/scylladb/scylla-migrator/releases/generate-notes",
                        "/repos/scylladb/scylla-migrator/releases"])
                else:
                    self.assertEqual(writes, [])
                    self.assertEqual(result.returncode == 0, scenario == "published", result.stderr)
                if scenario == "draft-page-two":
                    self.assertIn("unpublished", result.stderr)
                    self.assertEqual(calls[-1][1], "/repos/scylladb/scylla-migrator/releases?per_page=100&page=2")


if __name__ == "__main__":
    unittest.main()
