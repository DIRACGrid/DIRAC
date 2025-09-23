import base64
import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from DIRAC.Core.Security.DiracX import diracxTokenFromPEM, PEM_BEGIN, PEM_END, RE_DIRACX_PEM


class TestDiracxTokenFromPEM:
    """Test cases for diracxTokenFromPEM function"""

    def create_valid_token_data(self):
        """Create valid token data for testing"""
        return {
            "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
            "refresh_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.refresh",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

    def create_pem_content(self, token_data=None, include_other_content=True):
        """Create PEM content with embedded DiracX token"""
        if token_data is None:
            token_data = self.create_valid_token_data()

        # Encode token data
        token_json = json.dumps(token_data)
        encoded_token = base64.b64encode(token_json.encode("utf-8")).decode()

        # Create PEM content
        pem_content = ""
        if include_other_content:
            pem_content += "-----BEGIN CERTIFICATE-----\n"
            pem_content += "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n"
            pem_content += "-----END CERTIFICATE-----\n"

        pem_content += f"{PEM_BEGIN}\n"
        pem_content += encoded_token + "\n"
        pem_content += f"{PEM_END}\n"

        return pem_content

    def test_valid_token_extraction(self):
        """Test successful extraction of valid token from PEM file"""
        token_data = self.create_valid_token_data()
        pem_content = self.create_pem_content(token_data)

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
            f.write(pem_content)
            temp_path = f.name

        try:
            result = diracxTokenFromPEM(temp_path)
            assert result == token_data
        finally:
            Path(temp_path).unlink()

    def test_no_token_in_pem(self):
        """Test behavior when no DiracX token is present in PEM file"""
        pem_content = """-----BEGIN CERTIFICATE-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
-----END CERTIFICATE-----"""

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
            f.write(pem_content)
            temp_path = f.name

        try:
            result = diracxTokenFromPEM(temp_path)
            assert result is None
        finally:
            Path(temp_path).unlink()

    def test_multiple_tokens_error(self):
        """Test that multiple tokens raise ValueError"""
        token_data = self.create_valid_token_data()

        # Create PEM with two tokens
        pem_content = self.create_pem_content(token_data)
        pem_content += "\n" + self.create_pem_content(token_data, include_other_content=False)

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
            f.write(pem_content)
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Found multiple DiracX tokens"):
                diracxTokenFromPEM(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_malformed_base64(self):
        """Test behavior with malformed base64 data"""
        pem_content = f"""{PEM_BEGIN}
invalid_base64_data_that_will_cause_error!
{PEM_END}"""

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
            f.write(pem_content)
            temp_path = f.name

        try:
            with pytest.raises(Exception):  # base64.b64decode will raise an exception
                diracxTokenFromPEM(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_invalid_json_in_token(self):
        """Test behavior with invalid JSON in token data"""
        invalid_json = "this is not valid json"
        encoded_invalid = base64.b64encode(invalid_json.encode("utf-8")).decode()

        pem_content = f"""{PEM_BEGIN}
{encoded_invalid}
{PEM_END}"""

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
            f.write(pem_content)
            temp_path = f.name

        try:
            with pytest.raises(json.JSONDecodeError):
                diracxTokenFromPEM(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_token_with_unicode_characters(self):
        """Test token with unicode characters"""
        unicode_token = {
            "access_token": "token_with_unicode_ñ_é_ü",
            "refresh_token": "refresh_with_emoji_🚀_🎉",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        pem_content = self.create_pem_content(unicode_token)

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem") as f:
            f.write(pem_content)
            temp_path = f.name

        try:
            result = diracxTokenFromPEM(temp_path)
            assert result == unicode_token
        finally:
            Path(temp_path).unlink()

    def test_regex_pattern_validation(self):
        """Test that the regex pattern correctly identifies DiracX tokens"""
        # Test that the regex matches the expected pattern
        token_data = self.create_valid_token_data()
        token_json = json.dumps(token_data)
        encoded_token = base64.b64encode(token_json.encode("utf-8")).decode()

        test_content = f"{PEM_BEGIN}\n{encoded_token}\n{PEM_END}"
        matches = RE_DIRACX_PEM.findall(test_content)

        assert len(matches) == 1
        assert matches[0] == encoded_token
