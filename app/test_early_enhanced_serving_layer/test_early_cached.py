"""
Unit tests for the `cached` decorator in enhanced_serving_layer.py.

Covers:
- Happy paths: correct caching, TTL expiry, argument variations.
- Edge cases: None/empty args, unhashable args, exceptions, TTL=0, large TTL, kwargs order, etc.

Assumes tests are a sibling to app/enhanced_serving_layer.py.
"""

import pytest
import time
import types
from functools import wraps

from app.enhanced_serving_layer import cached, cache, cache_timestamps, CACHE_TTL_SECONDS

@pytest.fixture(autouse=True)
def clear_cache():
    """Ensure cache and timestamps are cleared before each test."""
    cache.clear()
    cache_timestamps.clear()
    yield
    cache.clear()
    cache_timestamps.clear()

class TestCached:
    # -------------------- HAPPY PATHS --------------------
    @pytest.mark.happy_path
    def test_caches_result_with_default_ttl(self):
        """Test that the decorator caches the result and returns cached value within TTL."""
        call_count = {'count': 0}

        @cached()
        def foo(x):
            call_count['count'] += 1
            return x * 2

        result1 = foo(5)
        result2 = foo(5)
        assert result1 == 10
        assert result2 == 10
        assert call_count['count'] == 1  # Only called once due to cache

    @pytest.mark.happy_path
    def test_caches_result_with_custom_ttl(self):
        """Test that the decorator uses custom TTL if provided."""
        call_count = {'count': 0}

        @cached(ttl_seconds=2)
        def bar(x):
            call_count['count'] += 1
            return x + 1

        result1 = bar(3)
        time.sleep(1)
        result2 = bar(3)
        assert result1 == result2
        assert call_count['count'] == 1

        time.sleep(2)
        result3 = bar(3)
        assert result3 == 4
        assert call_count['count'] == 2  # Called again after TTL expiry

    @pytest.mark.happy_path
    def test_cache_key_differs_for_args(self):
        """Test that different arguments produce different cache entries."""
        call_count = {'count': 0}

        @cached()
        def baz(x):
            call_count['count'] += 1
            return x

        assert baz(1) == 1
        assert baz(2) == 2
        assert call_count['count'] == 2
        assert baz(1) == 1
        assert call_count['count'] == 2  # Second call to baz(1) is cached

    @pytest.mark.happy_path
    def test_cache_key_differs_for_kwargs(self):
        """Test that different kwargs produce different cache entries."""
        call_count = {'count': 0}

        @cached()
        def qux(x, y=0):
            call_count['count'] += 1
            return x + y

        assert qux(1, y=2) == 3
        assert qux(1, y=3) == 4
        assert call_count['count'] == 2
        assert qux(1, y=2) == 3
        assert call_count['count'] == 2  # Second call to qux(1, y=2) is cached

    @pytest.mark.happy_path
    def test_cache_key_is_independent_of_kwargs_order(self):
        """Test that kwargs order does not affect cache key."""
        call_count = {'count': 0}

        @cached()
        def func(a=1, b=2):
            call_count['count'] += 1
            return a + b

        assert func(a=1, b=2) == 3
        assert func(b=2, a=1) == 3
        assert call_count['count'] == 1  # Both calls use same cache key

    # -------------------- EDGE CASES --------------------
    @pytest.mark.edge_case
    def test_cache_with_none_and_empty_args(self):
        """Test caching works with None and empty arguments."""
        call_count = {'count': 0}

        @cached()
        def foo(x=None):
            call_count['count'] += 1
            return x

        assert foo() is None
        assert foo() is None
        assert call_count['count'] == 1

    @pytest.mark.edge_case
    def test_cache_with_unhashable_args(self):
        """Test caching works with unhashable arguments (lists, dicts)."""
        call_count = {'count': 0}

        @cached()
        def foo(x):
            call_count['count'] += 1
            return sum(x)

        # Should not raise error
        assert foo([1, 2, 3]) == 6
        assert foo([1, 2, 3]) == 6
        assert call_count['count'] == 1

    @pytest.mark.edge_case
    def test_cache_with_kwargs_containing_unhashable(self):
        """Test caching works with kwargs containing unhashable values."""
        call_count = {'count': 0}

        @cached()
        def foo(x=0, y=None):
            call_count['count'] += 1
            return x + (sum(y) if y else 0)

        assert foo(x=1, y=[2, 3]) == 6
        assert foo(x=1, y=[2, 3]) == 6
        assert call_count['count'] == 1

    @pytest.mark.edge_case
    def test_cache_ttl_zero(self):
        """Test that TTL=0 disables caching (always recomputes)."""
        call_count = {'count': 0}

        @cached(ttl_seconds=0)
        def foo(x):
            call_count['count'] += 1
            return x * 2

        assert foo(1) == 2
        assert foo(1) == 2
        assert call_count['count'] == 2  # No caching

    @pytest.mark.edge_case
    def test_cache_ttl_large(self):
        """Test that a large TTL keeps cache for a long time."""
        call_count = {'count': 0}

        @cached(ttl_seconds=10000)
        def foo(x):
            call_count['count'] += 1
            return x * 2

        assert foo(1) == 2
        time.sleep(0.1)
        assert foo(1) == 2
        assert call_count['count'] == 1

    @pytest.mark.edge_case
    def test_cache_decorator_on_method(self):
        """Test decorator works on instance methods."""
        class MyClass:
            def __init__(self):
                self.call_count = 0

            @cached()
            def method(self, x):
                self.call_count += 1
                return x * 3

        obj = MyClass()
        assert obj.method(2) == 6
        assert obj.method(2) == 6
        assert obj.call_count == 1

    @pytest.mark.edge_case
    def test_cache_decorator_preserves_function_metadata(self):
        """Test that the decorator preserves function name and docstring."""
        @cached()
        def foo(x):
            """Docstring here."""
            return x

        assert foo.__name__ == "foo"
        assert foo.__doc__ == "Docstring here."

    @pytest.mark.edge_case
    def test_cache_decorator_handles_exceptions(self):
        """Test that exceptions are not cached and function is retried."""
        call_count = {'count': 0}

        @cached()
        def foo(x):
            call_count['count'] += 1
            if x == 0:
                raise ValueError("Zero!")
            return x

        with pytest.raises(ValueError):
            foo(0)
        with pytest.raises(ValueError):
            foo(0)
        assert call_count['count'] == 2  # Not cached

    @pytest.mark.edge_case
    def test_cache_key_with_multiple_args_and_kwargs(self):
        """Test cache key uniqueness with multiple args and kwargs."""
        call_count = {'count': 0}

        @cached()
        def foo(a, b, c=1, d=2):
            call_count['count'] += 1
            return a + b + c + d

        assert foo(1, 2, c=3, d=4) == 10
        assert foo(1, 2, d=4, c=3) == 10
        assert call_count['count'] == 1  # Same cache key

        assert foo(1, 2, c=3, d=5) == 11
        assert call_count['count'] == 2

    @pytest.mark.edge_case
    def test_cache_key_with_different_function_names(self):
        """Test that cache keys are unique per function name."""
        call_count = {'a': 0, 'b': 0}

        @cached()
        def foo(x):
            call_count['a'] += 1
            return x

        @cached()
        def bar(x):
            call_count['b'] += 1
            return x

        assert foo(1) == 1
        assert bar(1) == 1
        assert foo(1) == 1
        assert bar(1) == 1
        assert call_count['a'] == 1
        assert call_count['b'] == 1

    @pytest.mark.edge_case
    def test_cache_key_with_args_that_are_equal_but_different_types(self):
        """Test that cache keys distinguish between e.g. 1 and '1'."""
        call_count = {'count': 0}

        @cached()
        def foo(x):
            call_count['count'] += 1
            return x

        assert foo(1) == 1
        assert foo('1') == '1'
        assert call_count['count'] == 2
        assert foo(1) == 1
        assert foo('1') == '1'
        assert call_count['count'] == 2