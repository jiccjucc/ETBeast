import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "bot.py").read_text()

class PortfolioChecks(unittest.TestCase):
    def test_python_parses(self):
        ast.parse(SOURCE)

    def test_token_is_read_from_environment(self):
        self.assertIn("os.getenv('DISCORD_TOKEN'", SOURCE)
        self.assertNotRegex(SOURCE, r"(?i)DISCORD_TOKEN\s*=\s*['\"][A-Za-z0-9_.-]{30,}['\"]")

    def test_key_workflows_exist(self):
        for name in ('addmanualhours','voidmanualhours','submitexpense','weeklypayroll','clockin','clockout'):
            self.assertIn(f"name='{name}'", SOURCE)

if __name__ == "__main__":
    unittest.main()
