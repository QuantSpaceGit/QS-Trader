"""Tests for init-library command."""

import shutil

import pytest
from click.testing import CliRunner

from qs_trader.cli.commands.init_library import init_library_command


@pytest.fixture
def runner():
    """Create a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def temp_lib_dir(tmp_path):
    """Create a temporary directory for library initialization."""
    lib_dir = tmp_path / "test_library"
    yield lib_dir
    # Cleanup
    if lib_dir.exists():
        shutil.rmtree(lib_dir)


class TestInitLibraryCommand:
    """Tests for the init-library command."""

    def test_init_all_types(self, runner, temp_lib_dir):
        """Test initializing library with all component types."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir)])

        assert result.exit_code == 0
        assert "Success" in result.output
        assert temp_lib_dir.exists()

        # Check directories were created
        assert (temp_lib_dir / "strategies").exists()
        assert (temp_lib_dir / "indicators").exists()
        assert (temp_lib_dir / "adapters").exists()
        assert (temp_lib_dir / "risk_policies").exists()

        # Check template files were created
        assert (temp_lib_dir / "strategies" / "my_strategy.py").exists()
        assert (temp_lib_dir / "indicators" / "my_indicator.py").exists()
        assert (temp_lib_dir / "adapters" / "my_adapter.py").exists()
        assert (temp_lib_dir / "risk_policies" / "my_policy.yaml").exists()

        # Check __init__.py files
        assert (temp_lib_dir / "strategies" / "__init__.py").exists()
        assert (temp_lib_dir / "indicators" / "__init__.py").exists()
        assert (temp_lib_dir / "adapters" / "__init__.py").exists()

        # Check README was created
        assert (temp_lib_dir / "README.md").exists()

    def test_init_strategy_only(self, runner, temp_lib_dir):
        """Test initializing library with only strategies."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir), "--type", "strategy"])

        assert result.exit_code == 0
        assert temp_lib_dir.exists()

        # Check only strategy files were created
        assert (temp_lib_dir / "strategies").exists()
        assert (temp_lib_dir / "strategies" / "my_strategy.py").exists()
        assert (temp_lib_dir / "strategies" / "__init__.py").exists()

        # Check other directories were not created
        assert not (temp_lib_dir / "indicators").exists()
        assert not (temp_lib_dir / "adapters").exists()
        assert not (temp_lib_dir / "risk_policies").exists()

    def test_init_multiple_types(self, runner, temp_lib_dir):
        """Test initializing library with multiple specific types."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir), "--type", "strategy", "--type", "indicator"])

        assert result.exit_code == 0
        assert temp_lib_dir.exists()

        # Check strategy and indicator files were created
        assert (temp_lib_dir / "strategies").exists()
        assert (temp_lib_dir / "indicators").exists()

        # Check other directories were not created
        assert not (temp_lib_dir / "adapters").exists()
        assert not (temp_lib_dir / "risk_policies").exists()

    def test_init_indicator_only(self, runner, temp_lib_dir):
        """Test initializing library with only indicators."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir), "--type", "indicator"])

        assert result.exit_code == 0
        assert (temp_lib_dir / "indicators" / "my_indicator.py").exists()

    def test_init_adapter_only(self, runner, temp_lib_dir):
        """Test initializing library with only adapters."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir), "--type", "adapter"])

        assert result.exit_code == 0
        assert (temp_lib_dir / "adapters" / "my_adapter.py").exists()

    def test_init_risk_policy_only(self, runner, temp_lib_dir):
        """Test initializing library with only risk policies."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir), "--type", "risk-policy"])

        assert result.exit_code == 0
        assert (temp_lib_dir / "risk_policies" / "my_policy.yaml").exists()
        # Risk policies don't need __init__.py
        assert not (temp_lib_dir / "risk_policies" / "__init__.py").exists()

    def test_init_existing_directory_prompt_no(self, runner, temp_lib_dir):
        """Test that existing directory prompts user and respects 'no' answer."""
        # Create directory with a file
        temp_lib_dir.mkdir(parents=True)
        (temp_lib_dir / "existing.txt").write_text("existing content")

        result = runner.invoke(init_library_command, [str(temp_lib_dir)], input="n\n")

        assert result.exit_code == 0
        assert "Cancelled" in result.output
        # Original file should still exist
        assert (temp_lib_dir / "existing.txt").exists()

    def test_init_existing_directory_prompt_yes(self, runner, temp_lib_dir):
        """Test that existing directory prompts user and respects 'yes' answer."""
        # Create directory with a file
        temp_lib_dir.mkdir(parents=True)
        (temp_lib_dir / "existing.txt").write_text("existing content")

        result = runner.invoke(init_library_command, [str(temp_lib_dir)], input="y\n")

        assert result.exit_code == 0
        assert "Success" in result.output
        # New files should be created
        assert (temp_lib_dir / "strategies" / "my_strategy.py").exists()

    def test_init_force_flag(self, runner, temp_lib_dir):
        """Test that --force flag skips prompt and overwrites files."""
        # Create directory with a file
        temp_lib_dir.mkdir(parents=True)
        (temp_lib_dir / "existing.txt").write_text("existing content")

        result = runner.invoke(init_library_command, [str(temp_lib_dir), "--force"])

        assert result.exit_code == 0
        assert "Success" in result.output
        assert "Continue" not in result.output  # Should not prompt
        # New files should be created
        assert (temp_lib_dir / "strategies" / "my_strategy.py").exists()

    def test_readme_content(self, runner, temp_lib_dir):
        """Test that README.md contains proper content."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir)])

        assert result.exit_code == 0
        readme_path = temp_lib_dir / "README.md"
        assert readme_path.exists()

        content = readme_path.read_text()
        assert "Custom QS-Trader Library" in content
        assert "Structure" in content
        assert "Usage" in content
        assert "config/qs_trader.yaml" in content

    def test_template_files_valid_python(self, runner, temp_lib_dir):
        """Test that generated Python files are valid."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir)])

        assert result.exit_code == 0

        # Try to compile Python files
        strategy_file = temp_lib_dir / "strategies" / "my_strategy.py"
        indicator_file = temp_lib_dir / "indicators" / "my_indicator.py"
        adapter_file = temp_lib_dir / "adapters" / "my_adapter.py"

        # Should not raise SyntaxError
        compile(strategy_file.read_text(), str(strategy_file), "exec")
        compile(indicator_file.read_text(), str(indicator_file), "exec")
        compile(adapter_file.read_text(), str(adapter_file), "exec")

    def test_template_files_contain_expected_classes(self, runner, temp_lib_dir):
        """Test that generated files contain expected class definitions."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir)])

        assert result.exit_code == 0

        strategy_content = (temp_lib_dir / "strategies" / "my_strategy.py").read_text()
        assert "class MyStrategy(Strategy[MyStrategyConfig]):" in strategy_content
        assert "def on_bar" in strategy_content

        indicator_content = (temp_lib_dir / "indicators" / "my_indicator.py").read_text()
        assert "class MyIndicator(BaseIndicator):" in indicator_content
        assert "def calculate" in indicator_content

        adapter_content = (temp_lib_dir / "adapters" / "my_adapter.py").read_text()
        assert "class MyDataAdapter:" in adapter_content
        assert "def load_bars" in adapter_content

    def test_risk_policy_valid_yaml(self, runner, temp_lib_dir):
        """Test that generated risk policy YAML is valid."""
        import yaml

        result = runner.invoke(init_library_command, [str(temp_lib_dir)])

        assert result.exit_code == 0

        policy_file = temp_lib_dir / "risk_policies" / "my_policy.yaml"
        assert policy_file.exists()

        # Should parse without errors
        with policy_file.open() as f:
            policy_data = yaml.safe_load(f)

        assert policy_data is not None
        assert "name" in policy_data
        assert "sizing" in policy_data
        assert "limits" in policy_data

    def test_expanduser_in_path(self, runner, tmp_path):
        """Test that ~ in path is expanded correctly."""
        # Create a path with ~ (simulate home directory)
        result = runner.invoke(init_library_command, [str(tmp_path / "test_lib")])

        assert result.exit_code == 0
        assert (tmp_path / "test_lib").exists()

    def test_output_contains_next_steps(self, runner, temp_lib_dir):
        """Test that output contains helpful next steps."""
        result = runner.invoke(init_library_command, [str(temp_lib_dir)])

        assert result.exit_code == 0
        assert "Next steps:" in result.output
        assert "config/qs_trader.yaml" in result.output
        assert "custom_libraries:" in result.output
