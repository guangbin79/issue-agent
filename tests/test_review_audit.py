"""agora-review-audit 契约测试：机械三检＋元评审退出码（真 git 夹具；
LLM 面经 AGORA_REVIEW_MODEL 注入假 opencode 替身，零网络零 token）。"""
import os
import subprocess
import unittest
from pathlib import Path

PKG = Path(__file__).resolve().parent.parent
SCRIPT = PKG / "agora-review-audit"

FAKE_OC = """#!/usr/bin/env bash
# 假 opencode：从 AGORA_FAKE_VERDICT 读裁决尾行
echo "(meta-review fake)"
echo "${AGORA_FAKE_VERDICT:-VERDICT: PASS}"
"""


class TestReviewAudit(unittest.TestCase):
    def setUp(self):
        import tempfile, shutil
        self.tmp = Path(tempfile.mkdtemp())
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        self._git("init", "-q", "-b", "main")
        self._git("-c", "user.email=t@t", "-c", "user.name=t",
                  "commit", "-q", "--allow-empty", "-m", "init")
        self.base = self._git("rev-parse", "HEAD").stdout.strip()
        (self.repo / "app.cpp").write_text("v2\n")
        (self.repo / "feature.cpp").write_text("new\n")
        self._git("add", "-A")
        self._git("-c", "user.email=t@t", "-c", "user.name=t",
                  "commit", "-q", "-m", "work")
        rl = self.repo / ".review-loop"
        rl.mkdir()
        (rl / ".last-reviewed").write_text(
            self._git("rev-parse", "HEAD").stdout.strip())
        (rl / ".review-base").write_text(self.base)
        self.fake_oc = self.tmp / "fake-oc"
        self.fake_oc.write_text(FAKE_OC)
        self.fake_oc.chmod(0o755)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _git(self, *args):
        return subprocess.run(["git", *args], cwd=self.repo, check=True,
                              capture_output=True, text=True)

    def _evidence(self, text):
        (self.repo / ".review-loop" / "20261118-1000-test.md").write_text(
            text, encoding="utf-8")

    def _run(self, fake_verdict=None):
        env = dict(os.environ,
                   AGORA_REVIEW_BASE=self.base,
                   AGORA_REVIEW_MODEL=str(self.fake_oc))
        # 假 opencode 以路径形式被 --model 引用不成立——脚本用 opencode run；
        # 测试经 PATH 前置注入同名 opencode 替身
        bindir = self.tmp / "bin"
        bindir.mkdir(exist_ok=True)
        oc = bindir / "opencode"
        oc.write_text(FAKE_OC.replace(
            "${AGORA_FAKE_VERDICT:-VERDICT: PASS}",
            fake_verdict or "VERDICT: PASS"))
        oc.chmod(0o755)
        env["PATH"] = f"{bindir}:{env['PATH']}"
        return subprocess.run(["bash", str(SCRIPT)], cwd=self.repo,
                              capture_output=True, text=True, env=env, timeout=60)

    def test_full_pass_double_green(self):
        self._evidence("app.cpp 行为核对；feature.cpp 新增无风险")
        r = self._run()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("双绿", r.stdout)

    def test_missing_evidence_fails_before_llm(self):
        (self.repo / ".review-loop" / "20261118-1000-test.md").unlink(
            missing_ok=True)
        r = self._run()
        self.assertEqual(r.returncode, 1)
        self.assertIn("证据缺失", r.stderr)

    def test_uncovered_file_fails_mechanically(self):
        self._evidence("只谈了 unrelated 完全无关内容")  # 词干 unrelated≥5 但不匹配
        r = self._run()
        self.assertEqual(r.returncode, 1)
        self.assertIn("未提及改动文件", r.stderr)

    def test_meta_review_fail_bounces(self):
        self._evidence("app.cpp 核对；feature.cpp 审查")
        r = self._run(fake_verdict="VERDICT: FAIL 证据注水")
        self.assertEqual(r.returncode, 1)
        self.assertIn("元评审判定自审不实", r.stderr)

    def test_meta_review_no_verdict_tool_fail(self):
        self._evidence("app.cpp 核对；feature.cpp 审查")
        r = self._run(fake_verdict="输出坏了没有裁决行")
        self.assertEqual(r.returncode, 125)
        self.assertIn("无 VERDICT", r.stderr)

    def test_base_missing_tool_fail(self):
        self._evidence("app.cpp 核对；feature.cpp 审查")
        env = dict(os.environ, AGORA_REVIEW_BASE="0" * 40,
                   PATH=os.environ["PATH"])
        r = subprocess.run(["bash", str(SCRIPT)], cwd=self.repo,
                           capture_output=True, text=True, env=env, timeout=30)
        self.assertEqual(r.returncode, 125)


if __name__ == "__main__":
    unittest.main()
