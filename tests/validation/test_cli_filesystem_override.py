"""Integration test for CLI validation filesystem override.

Validates that the CLI validation command forces filesystem artifact mode
regardless of the system config's default artifact_policy setting.
"""

from __future__ import annotations

from pathlib import Path


class TestCLIFilesystemOverride:
    """Test that CLI validation forces filesystem artifact mode."""

    def test_cli_contains_filesystem_override_code(self) -> None:
        """CLI source code should contain filesystem override before SequentialValidationRunner."""
        cli_path = Path(__file__).parent.parent.parent / "src" / "qs_trader" / "validation" / "cli.py"
        
        with open(cli_path, 'r') as f:
            content = f.read()
        
        # Verify the override code is present
        assert 'from copy import deepcopy' in content, \
            "CLI should import deepcopy for system config override"
        
        assert '_child_sys_cfg = deepcopy(get_system_config())' in content, \
            "CLI should deep-copy the system config"
        
        assert '_child_sys_cfg.output.artifact_policy.mode = "filesystem"' in content, \
            "CLI should override artifact_policy.mode to 'filesystem'"
        
        assert 'system_config=_child_sys_cfg' in content, \
            "CLI should pass system_config to SequentialValidationRunner"

    def test_cli_override_appears_before_runner_creation(self) -> None:
        """Filesystem override should appear before SequentialValidationRunner creation."""
        cli_path = Path(__file__).parent.parent.parent / "src" / "qs_trader" / "validation" / "cli.py"
        
        with open(cli_path, 'r') as f:
            lines = f.readlines()
        
        # Find the line numbers
        override_line = None
        runner_line = None
        
        for i, line in enumerate(lines):
            if '_child_sys_cfg.output.artifact_policy.mode = "filesystem"' in line:
                override_line = i
            if 'runner = SequentialValidationRunner(' in line and 'system_config=_child_sys_cfg' in line:
                runner_line = i
        
        assert override_line is not None, "Filesystem override code not found"
        assert runner_line is not None, "SequentialValidationRunner creation not found"
        assert override_line < runner_line, \
            "Filesystem override should appear before SequentialValidationRunner creation"
