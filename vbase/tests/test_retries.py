import logging
import time
import unittest
from unittest.mock import Mock
from vbase.utils.retries import with_retries


class TestWithRetries(unittest.TestCase):
    """Test cases for the with_retries function."""

    def test_successful_operation_first_attempt(self):
        """Test that a successful operation on the first attempt returns the result."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(return_value="success")
        
        result = with_retries(operation, logger, max_attempts=5, delay=0)
        
        assert result == "success"
        assert operation.call_count == 1
        logger.info.assert_called_once()

    def test_successful_operation_after_retries(self):
        """Test that operation succeeds after a few failures."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(side_effect=[
            Exception("Error 1"),
            Exception("Error 2"),
            "success"
        ])
        
        result = with_retries(operation, logger, max_attempts=5, delay=0)
        
        assert result == "success"
        assert operation.call_count == 3
        assert logger.info.call_count == 3
        assert logger.error.call_count == 2

    def test_all_attempts_fail(self):
        """Test that exception is raised when all attempts fail."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(side_effect=Exception("Persistent error"))
        
        with self.assertRaises(Exception):
            with_retries(operation, logger, max_attempts=3, delay=0)
        
        assert operation.call_count == 3
        assert logger.info.call_count == 3
        assert logger.error.call_count == 3

    def test_max_attempts_parameter(self):
        """Test that max_attempts parameter is respected."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(side_effect=Exception("Error"))
        
        with self.assertRaises(Exception):
            with_retries(operation, logger, max_attempts=2, delay=0)
        
        assert operation.call_count == 2

    def test_exponential_backoff_delay(self):
        """Test that exponential backoff is applied correctly."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(side_effect=[
            Exception("Error 1"),
            Exception("Error 2"),
            Exception("Error 3"),
            "success"
        ])
        
        # Use small delay to keep test fast
        delay = 0.01
        start_time = time.time()
        
        result = with_retries(operation, logger, max_attempts=5, delay=delay)
        
        elapsed_time = time.time() - start_time
        
        # Expected delays: 0.01 * 2^0 + 0.01 * 2^1 + 0.01 * 2^2 = 0.01 + 0.02 + 0.04 = 0.07
        expected_min_delay = delay * (2**0 + 2**1 + 2**2)  # 0.07
        expected_max_delay = expected_min_delay + 0.05  # Add tolerance for execution time
        
        assert result == "success"
        assert operation.call_count == 4
        assert elapsed_time >= expected_min_delay
        assert elapsed_time <= expected_max_delay

    def test_zero_delay(self):
        """Test that zero delay works without sleeping."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(side_effect=[
            Exception("Error 1"),
            Exception("Error 2"),
            "success"
        ])
        
        start_time = time.time()
        result = with_retries(operation, logger, max_attempts=5, delay=0)
        elapsed_time = time.time() - start_time
        
        assert result == "success"
        # Should complete almost instantly with delay=0
        assert elapsed_time < 0.1

    def test_operation_returns_none(self):
        """Test that operation can return None successfully."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(return_value=None)
        
        result = with_retries(operation, logger, max_attempts=3, delay=0)
        
        assert result is None
        assert operation.call_count == 1

    def test_operation_returns_various_types(self):
        """Test that operations can return different types of values."""
        logger = Mock(spec=logging.Logger)
        
        # Test with integer
        operation_int = Mock(return_value=42)
        assert with_retries(operation_int, logger, max_attempts=3, delay=0) == 42
        
        # Test with list
        operation_list = Mock(return_value=[1, 2, 3])
        assert with_retries(operation_list, logger, max_attempts=3, delay=0) == [1, 2, 3]
        
        # Test with dict
        operation_dict = Mock(return_value={"key": "value"})
        assert with_retries(operation_dict, logger, max_attempts=3, delay=0) == {"key": "value"}

    def test_logging_messages(self):
        """Test that appropriate logging messages are generated."""
        logger = Mock(spec=logging.Logger)
        error = Exception("Test error")
        operation = Mock(side_effect=[error, error, "success"])
        
        result = with_retries(operation, logger, max_attempts=5, delay=0)
        
        # Check that info was called for each attempt
        assert logger.info.call_count == 3
        logger.info.assert_any_call("Attempt: %s", 1)
        logger.info.assert_any_call("Attempt: %s", 2)
        logger.info.assert_any_call("Attempt: %s", 3)
        
        # Check that error was logged
        assert logger.error.call_count == 2

    def test_different_exception_types(self):
        """Test retry behavior with different exception types."""
        logger = Mock(spec=logging.Logger)
        
        # Test with ValueError
        operation = Mock(side_effect=[ValueError("Value error"), "success"])
        result = with_retries(operation, logger, max_attempts=3, delay=0)
        assert result == "success"
        
        # Test with RuntimeError being raised on all attempts
        operation_runtime = Mock(side_effect=RuntimeError("Runtime error"))
        with self.assertRaises(RuntimeError):
            with_retries(operation_runtime, logger, max_attempts=2, delay=0)

    def test_delay_calculation_with_multiple_retries(self):
        """Test delay calculation with specific retry pattern."""
        logger = Mock(spec=logging.Logger)
        operation = Mock(side_effect=[
            Exception("Error 1"),
            Exception("Error 2"),
            "success"
        ])
        
        delay = 0.02
        start_time = time.time()
        
        result = with_retries(operation, logger, max_attempts=5, delay=delay)
        
        elapsed_time = time.time() - start_time
        
        # First retry: delay * 2^0 = 0.02
        # Second retry: delay * 2^1 = 0.04
        # Total expected: 0.06 seconds
        expected_delay = delay * (2**0 + 2**1)
        
        assert result == "success"
        assert elapsed_time >= expected_delay
        # Allow for 50ms tolerance for execution overhead
        assert elapsed_time <= expected_delay + 0.05
